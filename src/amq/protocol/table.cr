require "./field"
require "./short_string"
require "./long_string"
require "./errors"

module AMQ
  module Protocol
    struct Table
      BYTEFORMAT = IO::ByteFormat::NetworkEndian

      def initialize(tuple : NamedTuple)
        @io = IO::Memory.new
        tuple.each do |key, value|
          @io.write_bytes(ShortString.new(key.to_s))
          write_field(value)
        end
      end

      def initialize(hash : Hash(String, Field))
        @io = IO::Memory.new
        hash.each do |key, value|
          @io.write_bytes(ShortString.new(key))
          write_field(value)
        end
      end

      def initialize(_nil : Nil)
        @io = IO::Memory.new(0)
      end

      def initialize(@io = IO::Memory.new(0))
      end

      def clone : self
        io = IO::Memory.new(@io.bytesize)
        io.write @io.to_slice
        Table.new io
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

      def fetch(key : String, &)
        @io.rewind
        while @io.pos < @io.bytesize
          if key == ShortString.from_io(@io)
            return read_field
          else
            skip_field
          end
        end
        yield
      end

      def has_key?(key : String) : Bool
        @io.rewind
        while @io.pos < @io.bytesize
          if key == ShortString.from_io(@io)
            return true
          else
            skip_field
          end
        end
        false
      end

      @[Deprecated("key must be string")]
      def has_key?(key)
        has_key?(key.to_s)
      end

      def each(& : (String, Field) -> Nil)
        @io.rewind
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          v = read_field
          yield k, v
        end
      end

      def size
        @io.rewind
        i = 0
        while @io.pos < @io.bytesize
          ShortString.skip(@io)
          skip_field
          i += 1
        end
        i
      end

      def any?(& : (String, Field) -> _) : Bool
        @io.rewind
        while @io.pos < @io.bytesize
          k = ShortString.from_io(@io)
          v = read_field
          return true if yield(k, v)
        end
        false
      end

      def all?(& : String, Field -> _) : Bool
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
          h[k] = read_field(table_to_h: true)
        end
        h
      end

      def to_json(json : JSON::Builder)
        json.object do
          @io.rewind
          while @io.pos < @io.bytesize
            key = ShortString.from_io(@io)
            value = read_field
            json.field key, value
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
          io << '='
          read_field.inspect(io)
          first = false
        end
        io << ')'
      end

      # Comparition on a semantic level, not on byte level
      def ==(other : self)
        return false if size != other.size
        each do |k, v|
          return false if other.fetch(k, nil) != v
        end
        true
      end

      # See `Object#hash(hasher)`
      def hash(hasher)
        # Copied from Hash#hash
        # The hash value must be the same regardless of the
        # order of the keys.
        result = hasher.result

        each do |key, value|
          copy = hasher
          copy = key.hash(copy)
          copy = value.hash(copy)
          result &+= copy.result
        end

        result.hash(hasher)
      end

      def delete(key : String)
        ensure_writeable
        @io.rewind
        while @io.pos < @io.bytesize
          start_pos = @io.pos
          if key == ShortString.from_io(@io)
            v = read_field
            length = @io.pos - start_pos
            (@io.buffer + start_pos).move_from(@io.buffer + @io.pos, @io.bytesize - @io.pos)
            @io.bytesize -= length
            return v
          end
          skip_field
        end
        nil
      end

      @[Deprecated("key must be string")]
      def delete(key)
        delete(key.to_s)
      end

      def to_io(io, format) : Nil
        io.write_bytes(@io.bytesize.to_u32, format)
        io.write @io.to_slice
      end

      def self.from_bytes(bytes, format) : self
        size = format.decode(UInt32, bytes[0, 4])
        mem = IO::Memory.new(bytes[4, size], writeable: false)
        new(mem)
      end

      def self.from_io(io, format, size : UInt32? = nil) : self
        size ||= UInt32.from_io(io, format)
        case io
        when IO::Memory
          if io.@writeable
            mem = IO::Memory.new(size)
            IO.copy(io, mem, size)
            new(mem)
          else
            bytes = io.to_slice[io.pos, size]
            io.pos += size
            new(IO::Memory.new(bytes, writeable: false))
          end
        else
          mem = IO::Memory.new(size)
          IO.copy(io, mem, size)
          new(mem)
        end
      end

      def bytesize
        sizeof(UInt32) + @io.bytesize
      end

      def reject!(& : String, Field -> _) : self
        ensure_writeable
        @io.rewind
        while @io.pos < @io.bytesize
          start_pos = @io.pos
          key = ShortString.from_io(@io)
          value = read_field
          if yield(key, value)
            length = @io.pos - start_pos
            (@io.buffer + start_pos).move_from(@io.buffer + @io.pos, @io.bytesize - @io.pos)
            @io.bytesize -= length
            @io.pos -= length
          end
        end
        self
      end

      def merge!(other : Hash(String, Field) | NamedTuple | self) : self
        ensure_writeable
        @io.rewind
        other.each do |key, value|
          delete(key.to_s)
          @io.skip_to_end
          @io.write_bytes(ShortString.new(key.to_s))
          write_field(value)
        end
        self
      end

      private def ensure_writeable
        return if @io.@writeable.as(Bool)
        writeable_io = IO::Memory.new(@io.bytesize)
        writeable_io.write @io.to_slice
        @io = writeable_io
      end

      private def write_field(value)
        case value
        when JSON::Any
          write_field(value.raw)
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
          prefix_size do
            value.each do |v|
              write_field(v)
            end
          end
        when Time
          @io.write_byte 'T'.ord.to_u8
          @io.write_bytes(value.to_unix.to_i64, BYTEFORMAT)
        when Table
          @io.write_byte 'F'.ord.to_u8
          @io.write_bytes value, BYTEFORMAT
        when Hash, NamedTuple
          @io.write_byte 'F'.ord.to_u8
          prefix_size do
            value.each do |k, v|
              ShortString.new(k.to_s).to_io(@io)
              write_field(v)
            end
          end
        when Nil
          @io.write_byte 'V'.ord.to_u8
        else raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def prefix_size(&)
        @io.write_bytes(0_u32, BYTEFORMAT)
        start_pos = @io.pos
        begin
          yield
        ensure
          end_pos = @io.pos
          bytesize = end_pos - start_pos
          @io.pos = start_pos - sizeof(UInt32)
          @io.write_bytes(bytesize.to_u32, BYTEFORMAT)
          @io.pos = end_pos
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
        when 'D' then @io.skip(1 + sizeof(Int32))
        when 'V' then @io.skip(0)
        else          raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def read_field(table_to_h = false) : Field
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
        when 'A' then read_array(table_to_h)
        when 'T' then Time.unix(Int64.from_io(@io, BYTEFORMAT))
        when 'F' then t = Table.from_io(@io, BYTEFORMAT); table_to_h ? t.to_h : t
        when 'D' then read_decimal
        when 'V' then nil
        else          raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def read_decimal : Float64
        scale = @io.read_byte || raise IO::EOFError.new
        value = Int32.from_io(@io, BYTEFORMAT)
        value / 10**scale
      end

      private def read_array(table_to_h = false)
        size = UInt32.from_io(@io, BYTEFORMAT)
        end_pos = @io.pos + size
        a = Array(Field).new
        while @io.pos < end_pos
          a << read_field(table_to_h)
        end
        a
      end

      private def read_slice
        size = UInt32.from_io(@io, BYTEFORMAT)
        bytes = Bytes.new(size)
        @io.read_fully bytes
        bytes
      end
    end
  end
end

class IO::Memory
  def bytesize=(value)
    @bytesize = value
  end
end

require "json/builder"
require "base64"

struct Slice
  # Encodes the slice as a base64 encoded string
  def to_json(json : JSON::Builder)
    json.string Base64.encode(self)
  end
end
