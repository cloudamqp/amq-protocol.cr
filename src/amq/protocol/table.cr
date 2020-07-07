module AMQ
  module Protocol
    struct Table
      BYTEFORMAT = IO::ByteFormat::NetworkEndian

      def initialize(hash : Hash(String, Field)?, @io = IO::Memory.new)
        hash.each do |key, value|
          @io.write_bytes(ShortString.new(key))
          write_field(value)
        end if hash
      end

      def initialize(@io = IO::Memory.new(0))
      end

      def clone
        Table.new @io.clone
      end

      def []?(key : String)
        fetch(key) { nil }
      end

      def [](key : String)
        fetch(key) { raise KeyError.new "Missing hash key: #{key.inspect}" }
      end

      def fetch(key : String, default : Field)
        fetch(key) { default }
      end

      def fetch(key : String)
        @io.rewind
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          return read_field if k == key
          skip_field
        end
        yield
      end

      def has_key?(key) : Bool
        @io.rewind
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          return true if k == key
          skip_field
        end
        false
      end

      def any?(&blk : (String, Field) -> _) : Bool
        @io.rewind
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          v = read_field
          return true if yield(k, v)
        end
        false
      end

      def all?(&blk : String, Field -> _) : Bool
        @io.rewind
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          v = read_field
          return false unless yield(k, v)
        end
        true
      end

      def empty?
        @io.empty?
      end

      def []=(key : String, value : Field)
        delete(key)
        @io.skip_to_end
        @io.write_bytes(ShortString.new(key))
        write_field(value)
      end

      def to_h
        @io.rewind
        h = Hash(String, Field).new
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          h[k] = read_field
        end
        h
      end

      def to_json(json)
        json.object do
          @io.rewind
          while @io.pos < @io.bytesize
            key = ShortString.from_io(@io)
            value = read_field
            json.field key do
              value.to_json(json)
            end
          end
        end
      end

      def inspect(io)
        io << {{@type.name.id.stringify}} << '('
        first = true
        @io.rewind
        while @io.pos < @io.bytesize
          io << ", " unless first
          io << '@' << ShortString.from_io(@io)
          io << '=' << read_field
          first = false
        end
        io << ')'
      end

      def buffer
        @io.buffer
      end

      def ==(other : self)
        return false if bytesize != other.bytesize
        buffer.memcmp(other.buffer, bytesize - sizeof(UInt32))
      end

      def delete(key)
        @io.rewind
        while @io.pos < @io.bytesize
          start_pos = @io.pos
          if key == ShortString.from_io(@io)
            v = read_field
            length = @io.pos - start_pos
            @io.peek.move_to(@io.to_slice[start_pos, @io.bytesize - start_pos])
            @io.bytesize -= length
            return v
          end
          skip_field
        end
        nil
      end

      def to_io(io, format) : Nil
        io.write_bytes(@io.bytesize.to_u32, format)
        @io.rewind
        IO.copy(@io, io, @io.bytesize)
      end

      def self.from_io(io, format, size : UInt32? = nil) : self
        size ||= UInt32.from_io(io, format)
        mem = IO::Memory.new(size)
        IO.copy(io, mem, size)
        self.new(mem)
      end

      def bytesize
        sizeof(UInt32) + @io.bytesize
      end

      private def write_field(value)
        case value
        when Bool
          @io.write_byte 't'.ord.to_u8
          @io.write_byte(value ? 1_u8 : 0_u8)
        when Int8
          @io.write_byte 'b'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when UInt8
          @io.write_byte 'B'.ord.to_u8
          @io.write_byte(value)
        when Int16
          @io.write_byte 's'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when UInt16
          @io.write_byte 'u'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when Int32
          @io.write_byte 'I'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when UInt32
          @io.write_byte 'i'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when Int64
          @io.write_byte 'l'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when Float32
          @io.write_byte 'f'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when Float64
          @io.write_byte 'd'.ord.to_u8
          @io.write_bytes(value, BYTEFORMAT)
        when String
          @io.write_byte 'S'.ord.to_u8
          @io.write_bytes LongString.new(value), BYTEFORMAT
        when Bytes
          @io.write_byte 'x'.ord.to_u8
          @io.write_bytes(value.bytesize.to_u32, BYTEFORMAT)
          @io.write value
        when Array
          @io.write_byte 'A'.ord.to_u8
          length_pos = @io.pos
          @io.write_bytes(0_u32, BYTEFORMAT)
          start_pos = @io.pos
          value.each { |v| write_field(v) }
          end_pos = @io.pos
          @io.seek length_pos
          array_bytesize = end_pos - start_pos
          @io.write_bytes(array_bytesize.to_u32, BYTEFORMAT)
          @io.seek end_pos
        when Time
          @io.write_byte 'T'.ord.to_u8
          @io.write_bytes(value.to_unix.to_i64, BYTEFORMAT)
        when Table
          @io.write_byte 'F'.ord.to_u8
          @io.write_bytes value, BYTEFORMAT
        when Hash(String, Field)
          @io.write_byte 'F'.ord.to_u8
          @io.write_bytes Table.new(value), BYTEFORMAT
        when Nil
          @io.write_byte 'V'.ord.to_u8
        else raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def skip_field
        type = @io.read_byte
        case type
        when 't' then @io.skip(sizeof(UInt8))
        when 'b' then @io.skip(sizeof(Int8))
        when 'B' then @io.skip(sizeof(UInt8))
        when 's' then @io.skip(sizeof(Int16))
        when 'u' then @io.skip(sizeof(UInt16))
        when 'I' then @io.skip(sizeof(Int32))
        when 'i' then @io.skip(sizeof(UInt32))
        when 'l' then @io.skip(sizeof(Int64))
        when 'f' then @io.skip(sizeof(Float32))
        when 'd' then @io.skip(sizeof(Float64))
        when 'S' then @io.skip(UInt32.from_io(@io, BYTEFORMAT))
        when 'x' then @io.skip(UInt32.from_io(@io, BYTEFORMAT))
        when 'A' then @io.skip(UInt32.from_io(@io, BYTEFORMAT))
        when 'T' then @io.skip(sizeof(Int64))
        when 'F' then @io.skip(UInt32.from_io(@io, BYTEFORMAT))
        when 'V' then @io.skip(0)
        else          raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def read_field : Field
        type = @io.read_byte
        case type
        when 't' then @io.read_byte == 1_u8
        when 'b' then Int8.from_io(@io, BYTEFORMAT)
        when 'B' then UInt8.from_io(@io, BYTEFORMAT)
        when 's' then Int16.from_io(@io, BYTEFORMAT)
        when 'u' then UInt16.from_io(@io, BYTEFORMAT)
        when 'I' then Int32.from_io(@io, BYTEFORMAT)
        when 'i' then UInt32.from_io(@io, BYTEFORMAT)
        when 'l' then Int64.from_io(@io, BYTEFORMAT)
        when 'f' then Float32.from_io(@io, BYTEFORMAT)
        when 'd' then Float64.from_io(@io, BYTEFORMAT)
        when 'S' then LongString.from_io(@io, BYTEFORMAT)
        when 'x' then read_slice
        when 'A' then read_array
        when 'T' then Time.unix(Int64.from_io(@io, BYTEFORMAT))
        when 'F' then Table.from_io(@io, BYTEFORMAT)
        when 'V' then nil
        else          raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def read_array
        size = UInt32.from_io(@io, BYTEFORMAT)
        end_pos = @io.pos + size
        a = Array(Field).new
        while @io.pos < end_pos
          a << read_field
        end
        a
      end

      private def read_slice
        size = UInt32.from_io(@io, BYTEFORMAT)
        bytes = Bytes.new(size)
        @io.read_fully bytes
        bytes
      end

      class IO::Memory
        def bytesize=(value)
          @bytesize = value
        end

        def clone
          rewind
          io = IO::Memory.new bytesize
          IO.copy self, io
          io
        end
      end
    end
  end
end
