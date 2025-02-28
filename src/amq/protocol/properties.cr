require "./table"

module AMQ
  module Protocol
    struct Properties
      @[Flags]
      enum Has : UInt16
        ContentType     = 0x8000_u16
        ContentEncoding = 0x4000_u16
        Headers         = 0x2000_u16
        DeliveryMode    = 0x1000_u16
        Priority        = 0x0800_u16
        CorrelationID   = 0x0400_u16
        ReplyTo         = 0x0200_u16
        Expiration      = 0x0100_u16
        MessageID       = 0x0080_u16
        Timestamp       = 0x0040_u16
        Type            = 0x0020_u16
        UserID          = 0x0010_u16
        AppID           = 0x0008_u16
        Reserved1       = 0x0004_u16
      end

      property content_type
      property content_encoding
      property headers
      property delivery_mode
      property priority
      property correlation_id
      property reply_to
      property expiration
      property message_id
      property timestamp_raw : Int64?
      property type
      property user_id
      property app_id
      property reserved1

      def_equals_and_hash content_type, content_encoding, headers, delivery_mode,
        priority, correlation_id, reply_to, expiration, message_id, timestamp_raw,
        type, user_id, app_id, reserved1

      def initialize(@content_type : String? = nil,
                     @content_encoding : String? = nil,
                     @headers : Table? = nil,
                     @delivery_mode : UInt8? = nil,
                     @priority : UInt8? = nil,
                     @correlation_id : String? = nil,
                     @reply_to : String? = nil,
                     @expiration : String? = nil,
                     @message_id : String? = nil,
                     timestamp : Time | Int64 | Nil = nil,
                     @type : String? = nil,
                     @user_id : String? = nil,
                     @app_id : String? = nil,
                     @reserved1 : String? = nil)
        @timestamp_raw = timestamp.is_a?(Time) ? timestamp.to_unix : timestamp
      end

      def self.from_bytes(bytes : Bytes, format : IO::ByteFormat)
        pos = 0
        flags_int = format.decode(UInt16, bytes[pos, 2]); pos += 2
        has = Has.from_value?(flags_int) || raise Error::FrameDecode.new("Invalid property flags")

        if has.content_type?
          content_type = ShortString.from_bytes(bytes + pos, format); pos += 1 + content_type.bytesize
        end
        if has.content_encoding?
          content_encoding = ShortString.from_bytes(bytes + pos, format); pos += 1 + content_encoding.bytesize
        end
        if has.headers?
          headers = Table.from_bytes(bytes + pos, format); pos += headers.bytesize
        end
        if has.delivery_mode?
          delivery_mode = bytes[pos]; pos += 1
        end
        if has.priority?
          priority = bytes[pos]; pos += 1
        end
        if has.correlation_id?
          correlation_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + correlation_id.bytesize
        end
        if has.reply_to?
          reply_to = ShortString.from_bytes(bytes + pos, format); pos += 1 + reply_to.bytesize
        end
        if has.expiration?
          expiration = ShortString.from_bytes(bytes + pos, format); pos += 1 + expiration.bytesize
        end
        if has.message_id?
          message_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + message_id.bytesize
        end
        if has.timestamp?
          timestamp_raw = format.decode(Int64, bytes[pos, 8]); pos += 8
        end
        if has.type?
          type = ShortString.from_bytes(bytes + pos, format); pos += 1 + type.bytesize
        end
        if has.user_id?
          user_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + user_id.bytesize
        end
        if has.app_id?
          app_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + app_id.bytesize
        end
        if has.reserved1?
          reserved1 = ShortString.from_bytes(bytes + pos, format); pos += 1 + reserved1.bytesize
        end
        self.new(content_type, content_encoding, headers, delivery_mode,
          priority, correlation_id, reply_to, expiration,
          message_id, timestamp_raw, type, user_id, app_id, reserved1)
      end

      def self.from_io(io, format, flags = UInt16.from_io(io, format)) : self
        return self.new if flags.zero?
        has = Has.from_value?(flags) || raise Error::FrameDecode.new("Invalid property flags")
        content_type = ShortString.from_io(io, format) if has.content_type?
        content_encoding = ShortString.from_io(io, format) if has.content_encoding?
        headers = Table.from_io(io, format) if has.headers?
        delivery_mode = io.read_byte if has.delivery_mode?
        priority = io.read_byte if has.priority?
        correlation_id = ShortString.from_io(io, format) if has.correlation_id?
        reply_to = ShortString.from_io(io, format) if has.reply_to?
        expiration = ShortString.from_io(io, format) if has.expiration?
        message_id = ShortString.from_io(io, format) if has.message_id?
        timestamp_raw = Int64.from_io(io, format) if has.timestamp?
        type = ShortString.from_io(io, format) if has.type?
        user_id = ShortString.from_io(io, format) if has.user_id?
        app_id = ShortString.from_io(io, format) if has.app_id?
        reserved1 = ShortString.from_io(io, format) if has.reserved1?
        Properties.new(content_type, content_encoding, headers, delivery_mode,
          priority, correlation_id, reply_to, expiration,
          message_id, timestamp_raw, type, user_id, app_id, reserved1)
      end

      def self.from_json(data : JSON::Any) : self
        p = self.new
        p.content_type = data["content_type"]?.try(&.as_s)
        p.content_encoding = data["content_encoding"]?.try(&.as_s)
        p.headers = data["headers"]?.try(&.as_h?).try { |hdrs| Table.new hdrs }
        p.delivery_mode = data["delivery_mode"]?.try(&.as_i?.try(&.to_u8))
        p.priority = data["priority"]?.try(&.as_i?.try(&.to_u8))
        p.correlation_id = data["correlation_id"]?.try(&.as_s)
        p.reply_to = data["reply_to"]?.try(&.as_s)
        exp = data["expiration"]?
        p.expiration = exp.try { |e| e.as_s? || e.as_i64?.try(&.to_s) }
        p.message_id = data["message_id"]?.try(&.as_s)
        p.timestamp_raw = data["timestamp"]?.try(&.as_i64?)
        p.type = data["type"]?.try(&.as_s)
        p.user_id = data["user_id"]?.try(&.as_s)
        p.app_id = data["app_id"]?.try(&.as_s)
        p.reserved1 = data["reserved"]?.try(&.as_s)
        p
      end

      def to_json(json : JSON::Builder)
        json.object do
          if v = @content_type
            json.field "content_type", v
          end
          if v = @content_encoding
            json.field "content_encoding", v
          end
          if v = @headers
            json.field "headers", v
          end
          if v = @delivery_mode
            json.field "delivery_mode", v
          end
          if v = @priority
            json.field "priority", v
          end
          if v = @correlation_id
            json.field "correlation_id", v
          end
          if v = @reply_to
            json.field "reply_to", v
          end
          if v = @expiration
            json.field "expiration", v
          end
          if v = @message_id
            json.field "message_id", v
          end
          if v = @timestamp_raw
            json.field "timestamp", v
          end
          if v = @type
            json.field "type", v
          end
          if v = @user_id
            json.field "user_id", v
          end
          if v = @app_id
            json.field "app_id", v
          end
          if v = @reserved1
            json.field "reserved1", v
          end
        end
      end

      def to_io(io, format)
        has = Has::None
        has |= Has::ContentType if @content_type
        has |= Has::ContentEncoding if @content_encoding
        has |= Has::Headers if @headers
        has |= Has::DeliveryMode if @delivery_mode
        has |= Has::Priority if @priority
        has |= Has::CorrelationID if @correlation_id
        has |= Has::ReplyTo if @reply_to
        has |= Has::Expiration if @expiration
        has |= Has::MessageID if @message_id
        has |= Has::Timestamp if @timestamp_raw
        has |= Has::Type if @type
        has |= Has::UserID if @user_id
        has |= Has::AppID if @app_id
        has |= Has::Reserved1 if @reserved1
        io.write_bytes(has.to_u16, format)

        if s = @content_type
          io.write_bytes ShortString.new(s), format
        end
        if s = @content_encoding
          io.write_bytes ShortString.new(s), format
        end
        if h = @headers
          io.write_bytes h, format
        end
        if dm = @delivery_mode
          io.write_byte dm
        end
        if p = @priority
          io.write_byte p
        end
        if s = @correlation_id
          io.write_bytes ShortString.new(s), format
        end
        if s = @reply_to
          io.write_bytes ShortString.new(s), format
        end
        if s = @expiration
          io.write_bytes ShortString.new(s), format
        end
        if s = @message_id
          io.write_bytes ShortString.new(s), format
        end
        if ts = @timestamp_raw
          io.write_bytes ts, format
        end
        if s = @type
          io.write_bytes ShortString.new(s), format
        end
        if s = @user_id
          io.write_bytes ShortString.new(s), format
        end
        if s = @app_id
          io.write_bytes ShortString.new(s), format
        end
        if s = @reserved1
          io.write_bytes ShortString.new(s), format
        end
      end

      def bytesize
        size = 2
        if v = @content_type
          size += 1 + v.bytesize
        end
        if v = @content_encoding
          size += 1 + v.bytesize
        end
        if v = @headers
          size += v.bytesize
        end
        size += 1 if @delivery_mode
        size += 1 if @priority
        if v = @correlation_id
          size += 1 + v.bytesize
        end
        if v = @reply_to
          size += 1 + v.bytesize
        end
        if v = @expiration
          size += 1 + v.bytesize
        end
        if v = @message_id
          size += 1 + v.bytesize
        end
        size += sizeof(Int64) if @timestamp_raw
        if v = @type
          size += 1 + v.bytesize
        end
        if v = @user_id
          size += 1 + v.bytesize
        end
        if v = @app_id
          size += 1 + v.bytesize
        end
        if v = @reserved1
          size += 1 + v.bytesize
        end
        size
      end

      def self.skip(io, format) : Int64
        flags = io.read_bytes UInt16, format
        skipped = sizeof(UInt16).to_i64
        return skipped if flags.zero?
        has = Has.from_value?(flags) || raise Error::FrameDecode.new("Invalid property flags")
        if has.content_type?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.content_encoding?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.headers?
          len = UInt32.from_io(io, format)
          io.skip(len)
          skipped += sizeof(UInt32) + len
        end
        if has.delivery_mode?
          io.skip(1)
          skipped += 1
        end
        if has.priority?
          io.skip(1)
          skipped += 1
        end
        if has.correlation_id?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.reply_to?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.expiration?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.message_id?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.timestamp?
          io.skip(sizeof(Int64))
          skipped += sizeof(Int64)
        end
        if has.type?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.user_id?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.app_id?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if has.reserved1?
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        skipped
      end

      def clone
        self.new(
          @content_type.clone,
          @content_encoding.clone,
          @headers.clone,
          @delivery_mode.clone,
          @priority.clone,
          @correlation_id.clone,
          @reply_to.clone,
          @expiration.clone,
          @message_id.clone,
          @timestamp_raw.clone,
          @type.clone,
          @user_id.clone,
          @app_id.clone,
          @reserved1.clone
        )
      end

      # Parse the timestamp_raw value into a `Time`.
      # Assume it's in seconds since epoch, according to spec.
      # If that fails assume it's stored as milliseconds.
      # Else raise AMQ::Protocol::Error::DecodeFrame error.
      def timestamp : Time?
        if raw = @timestamp_raw
          if Int32::MIN <= raw <= Int32::MAX
            Time.unix(raw)
          else
            begin
              Time.unix_ms(raw)
            rescue ex : ArgumentError
              raise Error::FrameDecode.new("Could not parse timestamp value #{raw}", cause: ex)
            end
          end
        end
      end

      def timestamp=(value : Time?) : Nil
        @timestamp_raw = value.try &.to_unix
      end
    end
  end
end
