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
      property timestamp
      property type
      property user_id
      property app_id
      property reserved1

      def_equals_and_hash content_type, content_encoding, headers, delivery_mode,
        priority, correlation_id, reply_to, expiration, message_id, timestamp,
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
                     @timestamp : Time? = nil,
                     @type : String? = nil,
                     @user_id : String? = nil,
                     @app_id : String? = nil,
                     @reserved1 : String? = nil)
      end

      def self.from_io(io, format, bytesize = 2)
        flags = UInt16.from_io io, format
        invalid = false
        invalid ||= flags & 1_u16 << 0 > 0
        invalid ||= flags & 2_u16 << 0 > 0
        if invalid
          io.skip(bytesize - 2)
          raise Error::FrameDecode.new("Invalid property flags")
        end
        content_type = ShortString.from_io(io, format) if flags & FLAG_CONTENT_TYPE > 0
        content_encoding = ShortString.from_io(io, format) if flags & FLAG_CONTENT_ENCODING > 0
        headers = Table.from_io(io, format) if flags & FLAG_HEADERS > 0
        delivery_mode = io.read_byte if flags & FLAG_DELIVERY_MODE > 0
        priority = io.read_byte if flags & FLAG_PRIORITY > 0
        correlation_id = ShortString.from_io(io, format) if flags & FLAG_CORRELATION_ID > 0
        reply_to = ShortString.from_io(io, format) if flags & FLAG_REPLY_TO > 0
        expiration = ShortString.from_io(io, format) if flags & FLAG_EXPIRATION > 0
        message_id = ShortString.from_io(io, format) if flags & FLAG_MESSAGE_ID > 0
        timestamp = Time.unix(Int64.from_io(io, format)) if flags & FLAG_TIMESTAMP > 0
        type = ShortString.from_io(io, format) if flags & FLAG_TYPE > 0
        user_id = ShortString.from_io(io, format) if flags & FLAG_USER_ID > 0
        app_id = ShortString.from_io(io, format) if flags & FLAG_APP_ID > 0
        reserved1 = ShortString.from_io(io, format) if flags & FLAG_RESERVED1 > 0
        Properties.new(content_type, content_encoding, headers, delivery_mode,
          priority, correlation_id, reply_to, expiration,
          message_id, timestamp, type, user_id, app_id, reserved1)
      end

      def self.from_json(data : JSON::Any)
        p = Properties.new
        p.content_type = data["content_type"]?.try(&.as_s)
        p.content_encoding = data["content_encoding"]?.try(&.as_s)
        p.headers = data["headers"]?.try(&.as_h?)
          .try { |hdrs| Table.new self.cast_to_field(hdrs).as(Hash(String, Field)) }
        p.delivery_mode = data["delivery_mode"]?.try(&.as_i?.try(&.to_u8))
        p.priority = data["priority"]?.try(&.as_i?.try(&.to_u8))
        p.correlation_id = data["correlation_id"]?.try(&.as_s)
        p.reply_to = data["reply_to"]?.try(&.as_s)
        exp = data["expiration"]?
        p.expiration = exp.try { |e| e.as_s? || e.as_i64?.try(&.to_s) }
        p.message_id = data["message_id"]?.try(&.as_s)
        p.timestamp = data["timestamp"]?.try(&.as_i64?).try { |s| Time.unix(s) }
        p.type = data["type"]?.try(&.as_s)
        p.user_id = data["user_id"]?.try(&.as_s)
        p.app_id = data["app_id"]?.try(&.as_s)
        p.reserved1 = data["reserved"]?.try(&.as_s)
        p
      end

      # https://github.com/crystal-lang/crystal/issues/4885#issuecomment-325109328
      def self.cast_to_field(x : Array) : Field
        x.map { |e| cast_to_field(e).as(Field) }.as(Field)
      end

      def self.cast_to_field(x : Hash) : Field
        h = Hash(String, Field).new
        x.each do |(k, v)|
          h[k] = cast_to_field(v).as(Field)
        end
        h
      end

      def self.cast_to_field(x : JSON::Any) : Field
        if a = x.as_a?
          cast_to_field(a)
        elsif h = x.as_h?
          cast_to_field(h)
        else
          x.raw.as(Field)
        end
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
          "timestamp"        => @timestamp,
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
        flags = flags | FLAG_TIMESTAMP if @timestamp
        flags = flags | FLAG_TYPE if @type
        flags = flags | FLAG_USER_ID if @user_id
        flags = flags | FLAG_APP_ID if @app_id
        flags = flags | FLAG_RESERVED1 if @reserved1

        io.write_bytes(flags, format)

        io.write_bytes ShortString.new(@content_type.not_nil!), format if @content_type
        io.write_bytes ShortString.new(@content_encoding.not_nil!), format if @content_encoding
        io.write_bytes @headers.not_nil!, format if @headers
        io.write_byte @delivery_mode.not_nil! if @delivery_mode
        io.write_byte @priority.not_nil! if @priority
        io.write_bytes ShortString.new(@correlation_id.not_nil!), format if @correlation_id
        io.write_bytes ShortString.new(@reply_to.not_nil!), format if @reply_to
        io.write_bytes ShortString.new(@expiration.not_nil!), format if @expiration
        io.write_bytes ShortString.new(@message_id.not_nil!), format if @message_id
        io.write_bytes @timestamp.not_nil!.to_unix.to_i64, format if @timestamp
        io.write_bytes ShortString.new(@type.not_nil!), format if @type
        io.write_bytes ShortString.new(@user_id.not_nil!), format if @user_id
        io.write_bytes ShortString.new(@app_id.not_nil!), format if @app_id
        io.write_bytes ShortString.new(@reserved1.not_nil!), format if @reserved1
      end

      def bytesize
        size = 2
        size += 1 + @content_type.not_nil!.bytesize if @content_type
        size += 1 + @content_encoding.not_nil!.bytesize if @content_encoding
        size += @headers.not_nil!.bytesize if @headers
        size += 1 if @delivery_mode
        size += 1 if @priority
        size += 1 + @correlation_id.not_nil!.bytesize if @correlation_id
        size += 1 + @reply_to.not_nil!.bytesize if @reply_to
        size += 1 + @expiration.not_nil!.bytesize if @expiration
        size += 1 + @message_id.not_nil!.bytesize if @message_id
        size += sizeof(Int64) if @timestamp
        size += 1 + @type.not_nil!.bytesize if @type
        size += 1 + @user_id.not_nil!.bytesize if @user_id
        size += 1 + @app_id.not_nil!.bytesize if @app_id
        size += 1 + @reserved1.not_nil!.bytesize if @reserved1
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
          @timestamp.clone,
          @type.clone,
          @user_id.clone,
          @app_id.clone,
          @reserved1.clone
        )
      end
    end
  end
end
