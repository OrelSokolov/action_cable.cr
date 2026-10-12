# Connection: fiber-per-connection cable endpoint. Speaks the Rails
# ActionCable JSON protocol 1:1 so the official @rails/actioncable
# JS client works unchanged:
#   server → client: welcome | ping | confirm_subscription |
#                     reject_subscription | {identifier, message}
#   client → server: {command: subscribe|message|unsubscribe,
#                     identifier, data?}
#
# Outgoing frames go through CorkWriter (P1.5): batched into one
# write per flush cycle; stdlib auto-pongs serialize via LockedImageIO.

module ActionCable
  # Compile-time registry of connection identifiers, filled via
  # Connection.identified_by; read by #identifiers, Server and
  # RemoteConnections (the gem's Connection::Identification class
  # attribute).
  CONNECTION_IDENTIFIERS = [] of Nil

  # connect-хук отверг соединение (gem's UnauthorizedConnectionError).
  class UnauthorizedConnectionError < Exception
  end

  class Connection
    include ConnectionAPI

    PING_INTERVAL = 3.seconds

    @internal_channel_name : String?

    def initialize(@io : IO, @server : Server, @request : HTTP::Request? = nil)
      @iolock = Mutex.new
      @ws = HTTP::WebSocket.new(LockedImageIO.new(@io, @iolock), sync_close: false)
      @writer = CorkWriter.new(@io, @iolock)
      @subscriptions = {} of String => Channel::Base
      @closed = false
      @identifier_values = {} of Symbol => String
    end

    def run : Nil
      begin
        connect
      rescue ex : UnauthorizedConnectionError
        # The gem's rejection: disconnect frame with reason
        # "unauthorized", reconnect: false (wire: Internal cases).
        payload = {} of String => CableValue
        payload["type"] = Internal::MESSAGE_TYPES[:disconnect]
        payload["reason"] = Internal::DISCONNECT_REASONS[:unauthorized]
        payload["reconnect"] = false
        send_json(payload)
        @writer.flush
        @ws.close
        return
      end
      @ws.on_message { |msg| handle_command(msg) }
      @ws.on_close do
        @closed = true
        unsubscribe_from_internal_channel
        @subscriptions.each_value(&.unsubscribe_from_channel)
        @subscriptions.clear
        @server.unregister_connection(self)
        disconnect
        @writer.close # drain what is left, then stop the writer fiber
      end
      @server.register_connection(self)
      subscribe_to_internal_channel
      send_welcome
      spawn_ping_loop
      @ws.run # blocks this fiber until the socket closes
    end

    # Override (subclass or reopen) — called once on open, before the
    # welcome message; set identifier values here.
    def connect : Nil
    end

    # Override — called once the socket has closed, after channels and
    # the internal channel are unsubscribed.
    def disconnect : Nil
    end

    # Upgrade-запрос (доступен, когда connection создан ws_handler'ом;
    # в юнит-тестах может не быть).
    def request : HTTP::Request?
      @request
    end

    # raise UnauthorizedConnectionError из connect-хука — отвергнуть
    # соединение (gem: reject_unauthorized_connection!).
    def reject_unauthorized_connection! : Nil
      raise UnauthorizedConnectionError.new("An unauthorized connection attempt was rejected")
    end

    # Cookie-заголовки upgrade-запроса (gem: request.cookies через
    # ActionDispatch::Cookies в connection).
    def request_cookies : Hash(String, String)
      jar = {} of String => String
      header = @request.try(&.headers["Cookie"]?)
      return jar unless header
      header.split(';').each do |part|
        key, value = part.split('=', 2)
        raw = value.try(&.strip) || ""
        decoded = begin
          URI.decode_www_form(raw)
        rescue
          raw
        end
        jar[key.strip] = decoded
      end
      jar
    end

    # Верифицированная cookie-сессия фазы C (подписанный crails_session,
    # legacy-конверт + SHA1): значение warden-ключа devise и т.п. Доступно
    # в connect-хуке приложения:
    #
    #   def connect
    #     reject_unauthorized_connection! unless session["warden.user.user.key"]?
    #     self.current_user = ...
    #   end
    def session : Hash(String, String)
      @session ||= read_signed_session
    end

    @session : Hash(String, String)?

    private def read_signed_session : Hash(String, String)
      return {} of String => String unless @request
      return {} of String => String unless ActionController.cookies_secret_key_base
      cookies = ActionController::Cookies.new(build_cookie_context)
      if (value = cookies.signed[ActionController::Session::KEY]?)
        h = value.try(&.as_h?)
        return h.each_with_object({} of String => String) { |(k, v), acc| acc[k] = v.to_s } if h
      end
      {} of String => String
    rescue ex : RuntimeError
      # secret removed between requests (test seams): degrade to empty
      {} of String => String
    end

    # Минимальный контекст для чтения cookie джарбом action_pack.
    private def build_cookie_context : HTTP::Server::Context
      request = @request.not_nil!
      HTTP::Server::Context.new(request, HTTP::Server::Response.new(IO::Memory.new))
    end

    # --- ConnectionAPI (channel seam) --------------------------------
    def identifiers : Array(Symbol)
      ActionCable.connection_identifiers
    end

    # --- identified_by (gem's Connection::Identification) -------------
    # Marks a key as a connection identifier index so the connection can
    # be found later, e.g. for remote disconnect:
    #
    #   class ActionCable::Connection
    #     identified_by :current_user
    #   end
    #
    # Deviation: identifier values are Strings (the gid of an AR object
    # is its to_gid_param string anyway).
    macro identified_by(*names)
      {% for name in names %}
        {% unless ::ActionCable::CONNECTION_IDENTIFIERS.includes?(name) %}
          {% ::ActionCable::CONNECTION_IDENTIFIERS << name %}
          def {{name.id}} : String?
            @identifier_values[:{{name.id}}]?
          end

          def {{name.id}}=(value : String) : String
            @identifier_values[:{{name.id}}] = value
          end
        {% end %}
      {% end %}
    end

    # Single gid combining all registered identifier values, sorted and
    # colon-joined (gem's Connection#connection_identifier); nil when no
    # identifier has a value.
    def connection_identifier : String?
      gid = identifiers.compact_map { |id| @identifier_values[id]? }.map(&.to_s).sort.join(":")
      gid.empty? ? nil : gid
    end

    def transmit(cable : Hash(String | Symbol, CableValue)) : Nil
      payload = {} of String => CableValue
      payload["identifier"] = cable[:identifier]? || cable["identifier"]?
      payload["message"] = cable[:message]? || cable["message"]?
      send_json(payload)
    end

    def pubsub : SubscriptionAdapter::Base
      @server.pubsub
    end

    # --- fast-path subscription seam (P1.5) ---------------------------
    def subscribe_stream(stream : String, identifier : String) : Nil
      @server.pubsub.subscribe_fast(stream, self, identifier)
    end

    def unsubscribe_stream(stream : String) : Nil
      @server.pubsub.unsubscribe_fast(stream, self)
    end

    def dropped_messages : Int32
      @writer.dropped_messages
    end

    # Fast-path delivery: prebuilt envelope bytes from the adapter.
    def deliver_text(text : String) : Nil
      return if @closed
      @writer.deliver_text(text)
    end

    def buffered_amount : Int32
      @writer.buffered_amount
    end

    # --- protocol ----------------------------------------------------
    private def send_welcome : Nil
      send_json({"type" => Internal::MESSAGE_TYPES[:welcome]} of String => CableValue)
    end

    # --- internal channel (gem's Connection::InternalChannel) ---------
    # Per-connection pubsub channel the remote-connections disconnect
    # message arrives on: "action_cable/<connection_identifier>".
    def internal_channel : String?
      connection_identifier.try { |gid| ActionCable::INTERNAL_CHANNEL_PREFIX + gid }
    end

    private def subscribe_to_internal_channel : Nil
      return unless (name = internal_channel)
      @internal_channel_name = name
      pubsub.subscribe(name, ->(message : String) { process_internal_message(message) })
    end

    private def unsubscribe_from_internal_channel : Nil
      return unless (name = @internal_channel_name)
      @internal_channel_name = nil
      # Proc-identity deviation (see adapters): unsubscribe drops every
      # callback of the channel — with a shared identifier this also
      # detaches a sibling connection of the same gid.
      pubsub.unsubscribe(name, ->(_message : String) {})
    end

    private def process_internal_message(raw : String) : Nil
      message = JSON.parse(raw)
      if message["type"]?.try(&.as_s?) == Internal::MESSAGE_TYPES[:disconnect]
        reconnect = message["reconnect"]?.try(&.as_bool?)
        reconnect = true if reconnect.nil? # gem: message.fetch("reconnect", true)
        close_connection(Internal::DISCONNECT_REASONS[:remote], reconnect.not_nil!)
      end
    rescue ex : Exception
      # The gem closes the connection on any internal-channel error.
      close_connection
    end

    # Sends the disconnect frame (type/reason/reconnect) and starts the
    # WS close handshake — the gem's Connection#close.
    def close_connection(reason : String? = nil, reconnect : Bool = true) : Nil
      return if @closed
      payload = {} of String => CableValue
      payload["type"] = Internal::MESSAGE_TYPES[:disconnect]
      payload["reason"] = reason if reason
      payload["reconnect"] = reconnect
      send_json(payload)
      @writer.flush # disconnect frame must precede the close frame
      unsubscribe_from_internal_channel
      @ws.close
    end

    private def send_confirmation(identifier : String) : Nil
      send_json({
        "identifier" => identifier,
        "type"       => Internal::MESSAGE_TYPES[:confirmation],
      } of String => CableValue)
    end

    private def send_rejection(identifier : String) : Nil
      send_json({
        "identifier" => identifier,
        "type"       => Internal::MESSAGE_TYPES[:rejection],
      } of String => CableValue)
    end

    private def spawn_ping_loop : Nil
      spawn do
        until @closed
          sleep PING_INTERVAL
          break if @closed
          send_json({
            "type"    => Internal::MESSAGE_TYPES[:ping],
            "message" => Time.utc.to_unix_ms.to_s,
          } of String => CableValue)
        end
      end
    end

    private def handle_command(raw : String) : Nil
      data = JSON.parse(raw)
      command = data["command"]?.try &.as_s
      identifier = data["identifier"]?.try &.as_s

      case command
      when "subscribe"
        return unless identifier && !@subscriptions[identifier]?
        id_options = JSON.parse(identifier)
        klass = ActionCable.lookup_channel(id_options["channel"].as_s)
        if klass
          channel = klass.new(self, identifier, string_params(id_options))
          @subscriptions[identifier] = channel
          channel.subscribe_to_channel
          send_confirmation(identifier)
        else
          send_rejection(identifier)
        end
      when "message"
        return unless identifier && (channel = @subscriptions[identifier]?)
        channel.perform_action(string_params(JSON.parse(data["data"].as_s)))
      when "unsubscribe"
        if (channel = @subscriptions.delete(identifier))
          channel.unsubscribe_from_channel
        end
      end
    rescue ex : JSON::ParseException
      # Malformed frames are dropped, connection stays alive (the gem
      # logs and rescues per-command).
    end

    private def string_params(any : JSON::Any) : Hash(String, String)
      params = {} of String => String
      any.as_h.each { |k, v| params[k] = v.to_s }
      params
    rescue
      {} of String => String
    end

    private def send_json(payload : Hash(String, CableValue)) : Nil
      return if @closed
      @writer.deliver_text(payload.to_json)
    end
  end
end

require "http/web_socket"
require "./cork_writer"
