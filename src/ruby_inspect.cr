# Ruby-inspect formatting so oracle specs can diff against strings
# captured from the original gem (same approach as active_job.cr).
require "json"
require "./protocol"

module ActionCable
  module RubyInspect
    def self.inspect(value) : String
      case value
      when Hash
        return "{}" if value.empty?
        "{" + value.map { |k, v| entry_repr(k, v) }.join(", ") + "}"
      when Array
        "[" + value.map { |v| inspect(v) }.join(", ") + "]"
      when String
        "\"#{escape(value)}\""
      when JSON::Any
        any_inspect(value)
      when Nil   then "nil"
      when Bool  then value.to_s
      else            value.to_s
      end
    end

    private def self.entry_repr(key, value) : String
      case key
      when Symbol then "#{key}: #{inspect(value)}"
      else             "\"#{escape(key.to_s)}\" => #{inspect(value)}"
      end
    end

    # Ruby String#inspect escaping (covers what our oracle data needs).
    private def self.escape(s : String) : String
      s.gsub('\\', "\\\\\\\\").gsub('"', "\\\"")
    end

    private def self.any_inspect(value : JSON::Any) : String
      case raw = value.raw
      when Hash(String, JSON::Any)
        return "{}" if raw.empty?
        "{" + raw.map { |k, v| "\"#{escape(k)}\" => #{any_inspect(v)}" }.join(", ") + "}"
      when Array(JSON::Any)
        "[" + raw.map { |v| any_inspect(v) }.join(", ") + "]"
      when String
        "\"#{escape(raw)}\""
      else
        raw.to_s
      end
    end
  end
end
