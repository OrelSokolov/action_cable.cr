# ActionCable::RemoteConnections — the gem's remote_connections.rb 1:1:
#
#   ActionCable.server.remote_connections.where(current_user: "1").disconnect
#
# Finds connections by an identifier declared with `identified_by` and
# disconnects them — across all servers sharing the pubsub adapter,
# because the disconnect message rides the per-connection internal
# channel (Connection#internal_channel = "action_cable/<gid>").

module ActionCable
  # Prefix of the per-connection internal pubsub channel (the gem's
  # Connection::InternalChannel#internal_channel).
  INTERNAL_CHANNEL_PREFIX = "action_cable/"

  # Identifier names registered via Connection.identified_by, as a
  # runtime array (the compile-time macro list is CONNECTION_IDENTIFIERS).
  def self.connection_identifiers : Array(Symbol)
    ids = [] of Symbol
    {% for name in ::ActionCable::CONNECTION_IDENTIFIERS %}
      ids << {{ name }}
    {% end %}
    ids
  end

  class RemoteConnections
    def initialize(@server : Server)
    end

    # Find the remote connections of a user:
    #
    #   remote_connections.where(current_user: "1")
    #
    # Every identifier registered on the connection must be provided
    # (named arguments, String values); extra keys are ignored for the
    # channel lookup, mirroring the gem.
    def where(**ids : String) : RemoteConnection
      RemoteConnection.new(@server, ids.to_h)
    end

    # Exists solely for calling #disconnect on the found connection.
    class RemoteConnection
      class InvalidIdentifiersError < Exception; end

      def initialize(@server : Server, @ids : Hash(Symbol, String))
        missing = @server.connection_identifiers.reject { |id| @ids.has_key?(id) }
        unless missing.empty?
          raise InvalidIdentifiersError.new("Invalid identifier(s): #{missing.inspect}")
        end
      end

      # The internal channel the target connection(s) subscribe to.
      def internal_channel : String
        INTERNAL_CHANNEL_PREFIX + connection_gid
      end

      # Uses the internal channel to disconnect the connection(s).
      def disconnect(reconnect : Bool = true) : Nil
        @server.broadcast(internal_channel, {
          "type"      => Internal::MESSAGE_TYPES[:disconnect],
          "reconnect" => reconnect,
        }.to_json)
      end

      # Sorted colon-join of the registered identifiers' values — must
      # match Connection#connection_identifier byte for byte.
      private def connection_gid : String
        @server.connection_identifiers
          .map { |id| @ids[id]?.try(&.to_s) }
          .compact
          .sort
          .join(":")
      end
    end
  end
end
