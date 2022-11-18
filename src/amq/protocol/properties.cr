require "./table"

module AMQ
  module Protocol
    struct Properties
      FLAG_CONTENT_TYPE     = 0x8000_u16
      FLAG_CONTENT_ENCODING = 0x4000_u16
      FLAG_HEADERS          = 0x2000_u16
      FLAG_DELIVERY_MODE    = 0x1000_u16
      FLAG_PRIORITY         = 0x0800_u16
      FLAG_CORRELATION_ID   = 0x0400_u16
      FLAG_REPLY_TO         = 0x0200_u16
      FLAG_EXPIRATION       = 0x0100_u16
      FLAG_MESSAGE_ID       = 0x0080_u16
      FLAG_TIMESTAMP        = 0x0040_u16
      FLAG_TYPE             = 0x0020_u16
      FLAG_USER_ID          = 0x0010_u16
      FLAG_APP_ID           = 0x0008_u16
      FLAG_RESERVED1        = 0x0004_u16

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

      def self.from_bytes(bytes, format, bytesize = 2)
        pos = 0
        flags = format.decode(UInt16, bytes[pos, 2]); pos += 2
        invalid = false
        invalid ||= flags & 1_u16 << 0 > 0
        invalid ||= flags & 2_u16 << 0 > 0
        if invalid
          raise Error::FrameDecode.new("Invalid property flags")
        end

        if flags & FLAG_CONTENT_TYPE > 0
          content_type = ShortString.from_bytes(bytes + pos, format); pos += 1 + content_type.bytesize
        end
        if flags & FLAG_CONTENT_ENCODING > 0
          content_encoding = ShortString.from_bytes(bytes + pos, format); pos += 1 + content_encoding.bytesize
        end
        if flags & FLAG_HEADERS > 0
          headers = Table.from_bytes(bytes + pos, format); pos += headers.bytesize
        end
        if flags & FLAG_DELIVERY_MODE > 0
          delivery_mode = bytes[pos]; pos += 1
        end
        if flags & FLAG_PRIORITY > 0
          priority = bytes[pos]; pos += 1
        end
        if flags & FLAG_CORRELATION_ID > 0
          correlation_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + correlation_id.bytesize
        end
        if flags & FLAG_REPLY_TO > 0
          reply_to = ShortString.from_bytes(bytes + pos, format); pos += 1 + reply_to.bytesize
        end
        if flags & FLAG_EXPIRATION > 0
          expiration = ShortString.from_bytes(bytes + pos, format); pos += 1 + expiration.bytesize
        end
        if flags & FLAG_MESSAGE_ID > 0
          message_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + message_id.bytesize
        end
        if flags & FLAG_TIMESTAMP > 0
          timestamp_raw = format.decode(Int64, bytes[pos, 8]); pos += 8
        end
        if flags & FLAG_TYPE > 0
          type = ShortString.from_bytes(bytes + pos, format); pos += 1 + type.bytesize
        end
        if flags & FLAG_USER_ID > 0
          user_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + user_id.bytesize
        end
        if flags & FLAG_APP_ID > 0
          app_id = ShortString.from_bytes(bytes + pos, format); pos += 1 + app_id.bytesize
        end
        if flags & FLAG_RESERVED1 > 0
          reserved1 = ShortString.from_bytes(bytes + pos, format); pos += 1 + reserved1.bytesize
        end
        Properties.new(content_type, content_encoding, headers, delivery_mode,
          priority, correlation_id, reply_to, expiration,
          message_id, timestamp_raw, type, user_id, app_id, reserved1)
      end

      def self.from_io(io, format, flags = UInt16.from_io(io, format))
        invalid = false
        invalid ||= flags & 1_u16 << 0 > 0
        invalid ||= flags & 2_u16 << 0 > 0
        raise Error::FrameDecode.new("Invalid property flags") if invalid
        content_type = ShortString.from_io(io, format) if flags & FLAG_CONTENT_TYPE > 0
        content_encoding = ShortString.from_io(io, format) if flags & FLAG_CONTENT_ENCODING > 0
        headers = Table.from_io(io, format) if flags & FLAG_HEADERS > 0
        delivery_mode = io.read_byte if flags & FLAG_DELIVERY_MODE > 0
        priority = io.read_byte if flags & FLAG_PRIORITY > 0
        correlation_id = ShortString.from_io(io, format) if flags & FLAG_CORRELATION_ID > 0
        reply_to = ShortString.from_io(io, format) if flags & FLAG_REPLY_TO > 0
        expiration = ShortString.from_io(io, format) if flags & FLAG_EXPIRATION > 0
        message_id = ShortString.from_io(io, format) if flags & FLAG_MESSAGE_ID > 0
        timestamp_raw = Int64.from_io(io, format) if flags & FLAG_TIMESTAMP > 0
        type = ShortString.from_io(io, format) if flags & FLAG_TYPE > 0
        user_id = ShortString.from_io(io, format) if flags & FLAG_USER_ID > 0
        app_id = ShortString.from_io(io, format) if flags & FLAG_APP_ID > 0
        reserved1 = ShortString.from_io(io, format) if flags & FLAG_RESERVED1 > 0
        Properties.new(content_type, content_encoding, headers, delivery_mode,
          priority, correlation_id, reply_to, expiration,
          message_id, timestamp_raw, type, user_id, app_id, reserved1)
      end

      def self.from_json(data : JSON::Any) : self
        p = Properties.new
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
        {
          "content_type"     => @content_type,
          "content_encoding" => @content_encoding,
          "headers"          => @headers,
          "delivery_mode"    => @delivery_mode,
          "priority"         => @priority,
          "correlation_id"   => @correlation_id,
          "reply_to"         => @reply_to,
          "expiration"       => @expiration,
          "message_id"       => @message_id,
          "timestamp"        => @timestamp_raw,
          "type"             => @type,
          "user_id"          => @user_id,
          "app_id"           => @app_id,
          "reserved"         => @reserved1,
        }.compact.to_json(json)
      end

      def to_io(io, format)
        flags = 0_u16
        flags = flags | FLAG_CONTENT_TYPE if @content_type
        flags = flags | FLAG_CONTENT_ENCODING if @content_encoding
        flags = flags | FLAG_HEADERS if @headers
        flags = flags | FLAG_DELIVERY_MODE if @delivery_mode
        flags = flags | FLAG_PRIORITY if @priority
        flags = flags | FLAG_CORRELATION_ID if @correlation_id
        flags = flags | FLAG_REPLY_TO if @reply_to
        flags = flags | FLAG_EXPIRATION if @expiration
        flags = flags | FLAG_MESSAGE_ID if @message_id
        flags = flags | FLAG_TIMESTAMP if @timestamp_raw
        flags = flags | FLAG_TYPE if @type
        flags = flags | FLAG_USER_ID if @user_id
        flags = flags | FLAG_APP_ID if @app_id
        flags = flags | FLAG_RESERVED1 if @reserved1
        io.write_bytes(flags, format)

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
        if flags & FLAG_CONTENT_TYPE > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_CONTENT_ENCODING > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_HEADERS > 0
          len = UInt32.from_io(io, format)
          io.skip(len)
          skipped += sizeof(UInt32) + len
        end
        if flags & FLAG_DELIVERY_MODE > 0
          io.skip(1)
          skipped += 1
        end
        if flags & FLAG_PRIORITY > 0
          io.skip(1)
          skipped += 1
        end
        if flags & FLAG_CORRELATION_ID > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_REPLY_TO > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_EXPIRATION > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_MESSAGE_ID > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_TIMESTAMP > 0
          io.skip(sizeof(Int64))
          skipped += sizeof(Int64)
        end
        if flags & FLAG_TYPE > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_USER_ID > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_APP_ID > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        if flags & FLAG_RESERVED1 > 0
          len = io.read_byte || raise IO::EOFError.new
          io.skip(len)
          skipped += 1 + len
        end
        skipped
      end

      def clone
        Properties.new(
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
