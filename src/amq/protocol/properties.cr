require "./table"

module AMQ
  module Protocol
    struct Properties
      @[Flags]
      private enum Flags : UInt16
        ContentType     = 0x8000_u16
        ContentEncoding = 0x4000_u16
        Headers         = 0x2000_u16
        DeliveryMode    = 0x1000_u16
        Priority        = 0x0800_u16
        CorrelationId   = 0x0400_u16
        ReplyTo         = 0x0200_u16
        Expiration      = 0x0100_u16
        MessageId       = 0x0080_u16
        Timestamp       = 0x0040_u16
        Type            = 0x0020_u16
        UserId          = 0x0010_u16
        AppId           = 0x0008_u16
        Reserved1       = 0x0004_u16
      end

      def content_type
        if @has.content_type?
          io = @io.rewind
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def content_encoding
        has = @has
        if @has.content_encoding?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def headers
        has = @has
        if has.headers?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          Table.from_io(io, BYTEFORMAT)
        end
      end

      def delivery_mode
        has = @has
        if has.delivery_mode?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.read_byte || raise IO::EOFError.new
        end
      end

      def priority
        has = @has
        if has.priority?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.read_byte || raise IO::EOFError.new
        end
      end

      def correlation_id
        has = @has
        if has.correlation_id?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def reply_to
        has = @has
        if has.reply_to?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def expiration
        has = @has
        if has.expiration?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def message_id
        has = @has
        if has.message_id?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.expiration?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def timestamp
        has = @has
        if has.timestamp?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.expiration?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.message_id?
          Time.unix(Int64.from_io(io, BYTEFORMAT))
        end
      end

      def type
        has = @has
        if has.type?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.expiration?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.message_id?
          io.skip(sizeof(Int64)) if has.timestamp?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def user_id
        has = @has
        if has.user_id?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.expiration?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.message_id?
          io.skip(sizeof(Int64)) if has.timestamp?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.type?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def app_id
        has = @has
        if has.app_id?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.expiration?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.message_id?
          io.skip(sizeof(Int64)) if has.timestamp?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.user_id?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def reserved1
        has = @has
        if has.content_encoding?
          io = @io.rewind
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.content_encoding?
          io.skip(UInt32.from_io(io, BYTEFORMAT)) if has.headers?
          io.skip(1) if has.delivery_mode?
          io.skip(1) if has.priority?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.correlation_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.reply_to?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.expiration?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.message_id?
          io.skip(sizeof(Int64)) if has.timestamp?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.type?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.user_id?
          io.skip(io.read_byte || raise IO::EOFError.new) if has.app_id?
          ShortString.from_io(io, BYTEFORMAT)
        end
      end

      def ==(other : self)
        to_slice == other.to_slice
      end

      def to_slice
        @io.to_slice
      end

      getter has

      def initialize(content_type : String? = nil,
                     content_encoding : String? = nil,
                     headers : Table? = nil,
                     delivery_mode : UInt8? = nil,
                     priority : UInt8? = nil,
                     correlation_id : String? = nil,
                     reply_to : String? = nil,
                     expiration : String? = nil,
                     message_id : String? = nil,
                     timestamp : Time? = nil,
                     type : String? = nil,
                     user_id : String? = nil,
                     app_id : String? = nil,
                     reserved1 : String? = nil)
        @io = io = IO::Memory.new
        @has = Flags.new(0u16)
        @has |= Flags::ContentType if content_type
        @has |= Flags::ContentEncoding if content_encoding
        @has |= Flags::Headers if headers
        @has |= Flags::DeliveryMode if delivery_mode
        @has |= Flags::Priority if priority
        @has |= Flags::CorrelationId if correlation_id
        @has |= Flags::ReplyTo if reply_to
        @has |= Flags::Expiration if expiration
        @has |= Flags::MessageId if message_id
        @has |= Flags::Timestamp if timestamp
        @has |= Flags::Type if type
        @has |= Flags::UserId if user_id
        @has |= Flags::AppId if app_id
        @has |= Flags::Reserved1 if reserved1

        if v = content_type
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = content_encoding
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = headers
          io.write_bytes v, BYTEFORMAT
        end
        if v = delivery_mode
          io.write_byte v
        end
        if v = priority
          io.write_byte v
        end
        if v = correlation_id
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = reply_to
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = expiration
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = message_id
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = timestamp
          io.write_bytes v.to_unix.to_i64, BYTEFORMAT
        end
        if v = type
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = user_id
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = app_id
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
        if v = reserved1
          io.write_bytes ShortString.new(v), BYTEFORMAT
        end
      end

      def initialize(@has : Flags, @io = IO::Memory.new(0))
      end

      def self.from_io(io, format, bytesize : Int? = nil)
        raise ArgumentError.new("Only NetworkingEnding byte format supported") unless format == BYTEFORMAT
        flags = UInt16.from_io io, BYTEFORMAT
        has = Flags.new(flags)
        if bytesize
          mem = IO::Memory.new(bytesize - 2)
          IO.copy(io, mem, bytesize - 2)
        else
          mem = IO::Memory.new
          if has.content_type?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.content_encoding?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.headers?
            len = io.read_bytes UInt32, format
            mem.write_bytes len, format
            IO.copy(io, mem, len)
          end
          mem.write_byte(io.read_byte || raise IO::EOFError.new) if has.delivery_mode?
          mem.write_byte(io.read_byte || raise IO::EOFError.new) if has.priority?
          if has.correlation_id?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.reply_to?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.expiration?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.message_id?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          IO.copy(io, mem, sizeof(Int64)) if has.timestamp?
          if has.type?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.user_id?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.app_id?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
          if has.reserved1?
            len = io.read_byte || raise IO::EOFError.new
            mem.write_byte len
            IO.copy(io, mem, len)
          end
        end
        Properties.new(has, mem)
      end

      def self.from_json(data : JSON::Any)
        content_type = data["content_type"]?.try(&.as_s)
        content_encoding = data["content_encoding"]?.try(&.as_s)
        headers = data["headers"]?.try(&.as_h?)
          .try { |hdrs| Table.new self.cast_to_field(hdrs).as(Hash(String, Field)) }
        delivery_mode = data["delivery_mode"]?.try(&.as_i?.try(&.to_u8))
        priority = data["priority"]?.try(&.as_i?.try(&.to_u8))
        correlation_id = data["correlation_id"]?.try(&.as_s)
        reply_to = data["reply_to"]?.try(&.as_s)
        exp = data["expiration"]?
        expiration = exp.try { |e| e.as_s? || e.as_i64?.try(&.to_s) }
        message_id = data["message_id"]?.try(&.as_s)
        timestamp = data["timestamp"]?.try(&.as_i64?).try { |s| Time.unix(s) }
        type = data["type"]?.try(&.as_s)
        user_id = data["user_id"]?.try(&.as_s)
        app_id = data["app_id"]?.try(&.as_s)
        reserved1 = data["reserved"]?.try(&.as_s)
        Properties.new(content_type, content_encoding, headers, delivery_mode,
          priority, correlation_id, reply_to, expiration,
          message_id, timestamp, type, user_id, app_id, reserved1)
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
          "content_type"     => content_type,
          "content_encoding" => content_encoding,
          "headers"          => headers,
          "delivery_mode"    => delivery_mode,
          "priority"         => priority,
          "correlation_id"   => correlation_id,
          "reply_to"         => reply_to,
          "expiration"       => expiration,
          "message_id"       => message_id,
          "timestamp"        => timestamp,
          "type"             => type,
          "user_id"          => user_id,
          "app_id"           => app_id,
          "reserved"         => reserved1,
        }.compact.to_json(json)
      end

      def to_io(io, format)
        raise ArgumentError.new("Only NetworkingEnding byte format supported") unless format == BYTEFORMAT

        io.write_bytes(@has.to_u16, BYTEFORMAT)
        IO.copy(@io.rewind, io)
      end

      BYTEFORMAT = IO::ByteFormat::NetworkEndian

      def bytesize
        2 + @io.bytesize
      end

      def self.skip(io, format) : Int64
        raise ArgumentError.new("Only NetworkingEnding byte format supported") unless format == BYTEFORMAT

        flags = io.read_bytes UInt16, BYTEFORMAT
        has = Flags.new(flags)
        skipped = sizeof(UInt16).to_i64
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
          len = UInt32.from_io(io, BYTEFORMAT)
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
        Properties.new(
          content_type,
          content_encoding,
          headers,
          delivery_mode,
          priority,
          correlation_id,
          reply_to,
          expiration,
          message_id,
          timestamp,
          type,
          user_id,
          app_id,
          reserved1
        )
      end
    end
  end
end
