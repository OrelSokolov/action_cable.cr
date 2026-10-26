# ActionCable::Channel::Base — mirrors actioncable 8.1.4 channel
# semantics verified by oracle/expected.yml:
#   * transmit wraps the payload in {identifier:, message:}
#   * unsubscribe_from_channel clears streams before #unsubscribed
#   * perform_action dispatches to public subclass methods by name
#   * periodically registers timers started on subscribe, stopped on
#     unsubscribe (gem's Channel::PeriodicTimers)
#
require "active_record_cr"

# Deviations from the Ruby API (Crystal type system or port scope):
#   * transmit is public (Ruby keeps it private; no `send` in Crystal)
#   * streams exposed via #stream_names (Ruby: private #streams)
#   * subscription confirmation/rejection messages are sent by the
#     connection layer in 8.1 — the channel never transmits them, so
#     they are out of scope until the connection layer lands (P0.5)

module ActionCable
  # Compile-time registry of channel classes, filled via `inherited`.
  CHANNELS = [] of Nil

  # Имя приложения для gid-имен стримов — GlobalID.app из active_record_cr
  # (гем: globalid-railtie; serialize_broadcasting для записей — их
  # to_gid_param). Канонический вид: base64url("gid://<app>/<Class>/<id>")
  # без паддинга (оракул stream_for_expected.yml, sf_gid_param).

  # Аналог ActionCable.server: сервер по умолчанию для class-level
  # broadcast_to (приложение подменяет своим при монтировании /cable).
  @@server : Server?

  def self.server : Server
    @@server ||= Server.new
  end

  def self.server=(server : Server) : Server
    @@server = server
  end

  # Один broadcastable: AR-запись (gid), строка/число (как есть).
  alias Broadcastable = ActiveRecord::Base | String | Int32 | Int64

  def self.serialize_broadcasting(object : Broadcastable) : String
    case object
    when ActiveRecord::Base
      # gid-имя стрима записи — её to_gid_param (гем: broadcasting_for →
      # GlobalID), так имена стримов совпадают с turbo_stream_from.
      if object.id
        object.to_gid_param
      else
        object.to_s
      end
    else
      object.to_s
    end
  end

  # Compile-time registry of `periodically` declarations, filled via the
  # Channel::Base `periodically` macro; read by the end-of-program
  # codegen (macro finished below). Each entry: {class name, method
  # name, every}.
  PERIODIC_TIMER_DECLS = [] of Nil

  # Compile-time registry of on_subscribe/on_unsubscribe declarations
  # (gem's Channel::Callbacks: set_callback :subscribe, :after / :unsubscribe,
  # :before). Entries: {class name, kind, method name, unless-condition or nil}.
  CALLBACK_DECLS = [] of Nil

  # Seam implemented by the real connection layer (P0.5) and by test
  # fakes — the same identifiers/transmit/pubsub surface the gem's
  # own unit tests rely on.
  module ConnectionAPI
    abstract def identifiers : Array(Symbol)
    abstract def transmit(cable : Hash(String | Symbol, CableValue)) : Nil
    abstract def pubsub : SubscriptionAdapter::Base

    # Fast-path seam (P1.5): only reached when the pubsub adapter
    # advertises fast_broadcasts? — implemented by the real connection.
    def subscribe_stream(stream : String, identifier : String) : Nil
      raise NotImplementedError.new("subscribe_stream requires the WS connection layer")
    end

    def unsubscribe_stream(stream : String) : Nil
      raise NotImplementedError.new("unsubscribe_stream requires the WS connection layer")
    end
  end

  module Channel
    # One registered periodic timer (the gem's periodic_timers entries
    # `[callback, every: seconds]`): a method name on the channel plus
    # the firing interval.
    struct TimerSpec
      attr_reader method_name : String
      attr_reader every : Time::Span

      def initialize(@method_name : String, every : Time::Span | Number)
        span = every.is_a?(Time::Span) ? every : every.seconds
        if span <= Time::Span.zero
          raise ArgumentError.new("Expected every: to be a positive number of seconds, got #{every.inspect}")
        end
        @every = span
      end

      def every_seconds : Float64
        @every.total_seconds
      end
    end

    class Base
      attr_reader connection : ConnectionAPI
      attr_reader identifier : String
      attr_reader params : Hash(String, String)
      getter? unsubscribed = false

      @streams = {} of String => Proc(String, Nil)
      @fast_streams = [] of String
      @subscription_rejected = false
      @periodic_stop : ::Channel(Nil)? = nil

      def initialize(@connection : ConnectionAPI, @identifier : String,
                     @params : Hash(String, String) = {} of String => String)
      end

      macro inherited
        {% ::ActionCable::CHANNELS << @type %}
        # channel_name (gem's Channel::Naming): underscored class name
        # minus the Channel suffix, namespaces as single colons.
        def self.channel_name : String
          {{ @type.name.stringify.gsub(/Channel\z/, "").split("::").map(&.underscore).join(":") }}
        end
      end

      # Periodically performs a task while the channel is subscribed
      # (the gem's Channel::PeriodicTimers, timer semantics 1:1):
      # timers start after #subscribed (after_subscribe) and stop on
      # unsubscribe_from_channel (after_unsubscribe).
      #
      #   periodically :transmit_progress, every: 5.seconds
      #   periodically every: 3.minutes do
      #     transmit({action: "update_count"} of String => CableValue)
      #   end
      #
      # A method name or a block, plus the `every:` interval (a
      # Time::Span or a number of seconds). Deviation from the Ruby DSL:
      # this is a macro (class-body DSL), so the callable form is a
      # block or a method name only; the block runs as a generated
      # channel method with the channel as self, like Ruby's
      # instance_exec.
      macro periodically(method_name = nil, every = nil)
        {% raise "every: is required" unless every %}
        {% if method_name %}
          {% ::ActionCable::PERIODIC_TIMER_DECLS << { @type.name.stringify, method_name.id.stringify, every, false } %}
        {% else %}
          {% timer_method = "__periodic_timer_#{::ActionCable::PERIODIC_TIMER_DECLS.size}" %}
          def {{ timer_method.id }} : Nil
            {{ yield }}
          end
          {% ::ActionCable::PERIODIC_TIMER_DECLS << { @type.name.stringify, timer_method, every, true } %}
        {% end %}
      end

      # Timers registered for this class via `periodically` (generated
      # per subclass below; subclasses inherit ancestor timers).
      def self.periodic_timers : Array(TimerSpec)
        [] of TimerSpec
      end

      # Overridden per-subclass by end-of-program codegen (below): block
      # timers dispatch to their generated methods.
      def __fire_periodic(name : String) : Bool
        __dispatch_action(name, {} of String => String)
      end

      # on_subscribe :method, unless: :condition — runs after #subscribed
      # (gem's set_callback :subscribe, :after); on_unsubscribe :method —
      # runs before #unsubscribed (set_callback :unsubscribe, :before).
      # The unless-guard skips the callback when it returns truthy, e.g.
      # `unless: :subscription_rejected?` after a rejected subscription.
      macro on_subscribe(*args, **opts)
        {% cond = nil %}
        {% for k, v in opts %}{% if k.stringify == "unless" %}{% cond = v.id.stringify %}{% end %}{% end %}
        {% ::ActionCable::CALLBACK_DECLS << { @type.name.stringify, "subscribe", args[0].id.stringify, cond } %}
      end

      macro on_unsubscribe(*args, **opts)
        {% cond = nil %}
        {% for k, v in opts %}{% if k.stringify == "unless" %}{% cond = v.id.stringify %}{% end %}{% end %}
        {% ::ActionCable::CALLBACK_DECLS << { @type.name.stringify, "unsubscribe", args[0].id.stringify, cond } %}
      end

      # Override in subclasses — set up streams here.
      def subscribed : Nil
      end

      # Override in subclasses — cleanup here.
      def unsubscribed : Nil
      end

      def subscribe_to_channel : Nil
        subscribed
        __run_subscribe_callbacks
        start_periodic_timers
      end

      def unsubscribe_from_channel : Nil
        @unsubscribed = true
        stop_periodic_timers
        stop_all_streams
        __run_unsubscribe_callbacks
        unsubscribed
      end

      # Overridden per-subclass by end-of-program codegen (below): direct
      # calls to the declared on_subscribe/on_unsubscribe methods.
      def __run_subscribe_callbacks : Nil
      end

      def __run_unsubscribe_callbacks : Nil
      end

      private def start_periodic_timers : Nil
        timers = self.class.periodic_timers
        return if timers.empty?
        stop = @periodic_stop = ::Channel(Nil).new
        timers.each { |timer| spawn periodic_timer_loop(timer, stop) }
      end

      private def periodic_timer_loop(timer : TimerSpec, stop : ::Channel(Nil)) : Nil
        until unsubscribed?
          select
          when timeout(timer.every)
            break if unsubscribed?
            unless __fire_periodic(timer.method_name)
              raise ArgumentError.new("periodically :#{timer.method_name} — no public method #{timer.method_name} on #{self.class}")
            end
          when stop.receive
            break
          end
        end
      end

      private def stop_periodic_timers : Nil
        @periodic_stop.try(&.close)
        @periodic_stop = nil
      end

      # --- stream_for / broadcast_to (gem's Streams + Naming) ----------
      # broadcasting_for: [channel_name, *broadcastables].join(":"),
      # каждый элемент — gid AR-записи или to_s (оракул
      # stream_for_expected.yml).
      def self.broadcasting_for(broadcastables : Broadcastable | Array(Broadcastable)) : String
        parts = [channel_name]
        if broadcastables.is_a?(Array)
          broadcastables.each { |b| parts << ActionCable.serialize_broadcasting(b) }
        else
          parts << ActionCable.serialize_broadcasting(broadcastables)
        end
        parts.join(":")
      end

      def self.broadcast_to(broadcastables : Broadcastable | Array(Broadcastable),
                            payload : String | Hash(String, CableValue)) : Nil
        ActionCable.server.broadcast(broadcasting_for(broadcastables), payload)
      end

      def broadcasting_for(broadcastables : Broadcastable | Array(Broadcastable)) : String
        self.class.broadcasting_for(broadcastables)
      end

      def broadcast_to(broadcastables : Broadcastable | Array(Broadcastable),
                       payload : String | Hash(String, CableValue)) : Nil
        self.class.broadcast_to(broadcastables, payload)
      end

      # broadcast_to @room, action: :start, user: current_user.slice(:id, :name)
      # — Rails' kwargs form: the payload is built as a JSON object in
      # kwarg declaration order (NamedTuple preserves it).
      def broadcast_to(broadcastables : Broadcastable | Array(Broadcastable),
                       **payload) : Nil
        json = JSON.build do |j|
          j.object do
            payload.each do |key, value|
              j.field(key.to_s) { __broadcast_value_json(j, value) }
            end
          end
        end
        self.class.broadcast_to(broadcastables, json)
      end

      # Value members like PG::Numeric only implement to_json(io : IO), and
      # JSON::Builder is not an IO — each type is emitted explicitly instead
      # of dispatching to_json on the union (Object#to_json(io) would route
      # back through a builder and fail to compile).
      private def __broadcast_value_json(j : JSON::Builder, value) : Nil
        case value
        when String, Symbol then j.string(value.to_s)
        when Int64, Int32   then j.number(value)
        when Float64        then j.number(value)
        when Bool           then j.bool(value)
        when Nil            then j.null
        when Time           then j.string(value.to_rfc3339)
        when JSON::Any      then j.raw(value.to_json)
        when PG::Numeric    then j.number(value.to_f)
        when Array          then j.array { value.each { |v| __broadcast_value_json(j, v) } }
        when Hash           then j.object { value.each { |k, v| j.field(k.to_s) { __broadcast_value_json(j, v) } } }
        when Bytes          then j.array { value.each { |b| j.number(b) } }
        else
          j.string(value.to_s)
        end
        nil
      end

      def stream_for(broadcastables : Broadcastable | Array(Broadcastable),
                     callback : Proc(String, Nil)? = nil) : Nil
        stream_from(broadcasting_for(broadcastables), callback)
      end

      def stop_stream_for(broadcastables : Broadcastable | Array(Broadcastable)) : Nil
        if (callback = @streams.delete(broadcasting_for(broadcastables)))
          connection.pubsub.unsubscribe(broadcasting_for(broadcastables), callback)
        end
      end

      def stream_or_reject_for(broadcastables : Broadcastable | Array(Broadcastable) | Nil) : Nil
        if broadcastables
          stream_for(broadcastables)
        else
          reject
        end
      end

      def stream_from(broadcasting : String, callback : Proc(String, Nil)? = nil) : Nil
        # Fast path (P1.5): a transmit-only subscription registers
        # (connection, identifier) with the adapter so a broadcast can
        # encode the envelope once and fan out prebuilt bytes.
        if callback.nil? && connection.pubsub.fast_broadcasts?
          @fast_streams << broadcasting
          connection.subscribe_stream(broadcasting, @identifier)
        else
          cb = callback || default_stream_callback
          @streams[broadcasting] = cb
          connection.pubsub.subscribe(broadcasting, cb)
        end
      end

      def stop_all_streams : Nil
        @streams.each do |name, cb|
          connection.pubsub.unsubscribe(name, cb)
        end
        @streams.clear
        @fast_streams.each do |name|
          connection.unsubscribe_stream(name)
        end
        @fast_streams.clear
      end

      # Probe surface for the private Ruby #streams hash (name → callback).
      def stream_names : Array(String)
        @streams.keys + @fast_streams
      end

      def transmit(data : CableValue) : Nil
        envelope = {} of String | Symbol => CableValue
        envelope[:identifier] = @identifier
        envelope[:message] = data
        connection.transmit(envelope)
      end

      def reject : Nil
        @subscription_rejected = true
      end

      def rejected? : Bool
        @subscription_rejected
      end

      # Gem's name for rejected? (subscription_rejected? in Base).
      def subscription_rejected? : Bool
        @subscription_rejected
      end

      private def default_stream_callback : Proc(String, Nil)
        ->(msg : String) do
          payload = begin
            JSON.parse(msg)
          rescue
            JSON::Any.new(msg)
          end
          transmit(payload)
        end
      end

      def perform_action(data : Hash(String, String)) : Nil
        action = data["action"]?
        return unless action
        __dispatch_action(action, data)
      end

      # Overridden per-subclass by end-of-program codegen (below).
      def __dispatch_action(action : String, data : Hash(String, String)) : Bool
        false
      end
    end
  end
end

# End-of-program codegen: per-subclass action dispatch (methods are
# only known once the whole class body is parsed). A public 0-or-1-arg
# method — minus lifecycle callbacks — becomes an action, mirroring
# Ruby's action_methods + public_send(arity check).
  macro finished
    {% for c in ::ActionCable::CHANNELS %}
    class {{ c.id }}
      def self.periodic_timers : Array(::ActionCable::Channel::TimerSpec)
        specs = [] of ::ActionCable::Channel::TimerSpec
        {% for d in ::ActionCable::PERIODIC_TIMER_DECLS %}
          {% if d[0] == c.name.stringify || c.ancestors.any? { |a| a.name.stringify == d[0] } %}
            specs << ::ActionCable::Channel::TimerSpec.new({{ d[1] }}, {{ d[2] }})
          {% end %}
        {% end %}
        specs
      end

      # Periodic dispatch: block timers call their generated method
      # directly (macro-generated defs are not in c.methods, so
      # __dispatch_action can not see them); method-name timers reuse
      # the action dispatch.
      def __fire_periodic(name : String) : Bool
        {% for d in ::ActionCable::PERIODIC_TIMER_DECLS %}
          {% if d[3] && (d[0] == c.name.stringify || c.ancestors.any? { |a| a.name.stringify == d[0] }) %}
            if name == {{ d[1] }}
              {{ d[1].id }}
              return true
            end
          {% end %}
        {% end %}
        __dispatch_action(name, {} of String => String)
      end

      # on_subscribe/on_unsubscribe runners: direct calls (no `send` in
      # Crystal), honoring the unless-guard of each declaration.
      def __run_subscribe_callbacks : Nil
        {% for d in ::ActionCable::CALLBACK_DECLS %}
          {% if d[1] == "subscribe" && (d[0] == c.name.stringify || c.ancestors.any? { |a| a.name.stringify == d[0] }) %}
            {% if d[3] %}
              {{ d[2].id }} unless {{ d[3].id }}
            {% else %}
              {{ d[2].id }}
            {% end %}
          {% end %}
        {% end %}
      end

      def __run_unsubscribe_callbacks : Nil
        {% for d in ::ActionCable::CALLBACK_DECLS %}
          {% if d[1] == "unsubscribe" && (d[0] == c.name.stringify || c.ancestors.any? { |a| a.name.stringify == d[0] }) %}
            {% if d[3] %}
              {{ d[2].id }} unless {{ d[3].id }}
            {% else %}
              {{ d[2].id }}
            {% end %}
          {% end %}
        {% end %}
      end

      def __dispatch_action(action : String, data : Hash(String, String)) : Bool
        {% for m in c.methods %}
          {% if m.visibility != :private && m.visibility != :protected && m.name.stringify != "subscribed" && m.name.stringify != "unsubscribed" && m.name.stringify != "initialize" %}
            {% if m.args.size == 1 %}
              if action == {{ m.name.stringify }}
                {{ m.name.id }}(data)
                return true
              end
            {% elsif m.args.size == 0 %}
              if action == {{ m.name.stringify }}
                {{ m.name.id }}
                return true
              end
            {% end %}
          {% end %}
        {% end %}
        false
      end
    end
  {% end %}

  # Channel-class lookup by name (Ruby uses const_get; we generate the
  # table at end-of-program when all channel classes are known).
  def ActionCable.lookup_channel(name : String)
    {% for c in ::ActionCable::CHANNELS %}
      return {{ c.id }} if name == {{ c.name.stringify }}
    {% end %}
    nil
  end
end
