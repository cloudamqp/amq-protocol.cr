require "./field"
require "./errors"

module AMQ
  module Protocol
    struct Table
      BYTEFORMAT = IO::ByteFormat::NetworkEndian
      @buffer : Pointer(UInt8)
      @capacity = 0
      @bytesize = 0
      @pos = 0
      @read_only = false

      def initialize(hash : Hash(String, Field) | NamedTuple)
        hash.each { |k, v| @capacity += 1 + k.to_s.bytesize + 1 + capacity_required(v) }
        if @capacity.zero?
          @buffer = Pointer(UInt8).null
        else
          @buffer = GC.malloc_atomic(@capacity).as(UInt8*)
        end
        hash.each do |key, value|
          write_short_string(key.to_s)
          write_field(value)
        end
      end

      def initialize(bytes : Bytes? = Bytes.empty)
        bytes ||= Bytes.empty
        @buffer = bytes.to_unsafe
        @bytesize = @capacity = bytes.bytesize
        @read_only = true
      end

      def clone
        Table.new to_slice.clone
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
        @pos = 0
        while @pos < @bytesize
          if key == read_short_string
            return read_field
          else
            skip_field
          end
        end
        yield
      end

      def has_key?(key : String) : Bool
        @pos = 0
        while @pos < @bytesize
          if key == read_short_string
            return true
          else
            skip_field
          end
        end
        false
      end

      @[Deprecated("key must be String")]
      def has_key?(key)
        has_key?(key.to_s)
      end

      def each(& : (String, Field) -> Nil)
        @pos = 0
        while @pos < @bytesize
          k = read_short_string
          v = read_field
          yield k, v
        end
      end

      def each_key(& : String -> Nil)
        @pos = 0
        while @pos < @bytesize
          k = read_short_string
          skip_field
          yield k
        end
      end

      def size
        i = 0
        @pos = 0
        while @pos < @bytesize
          skip_short_string
          skip_field
          i += 1
        end
        i
      end

      def any?(& : (String, Field) -> _) : Bool
        @pos = 0
        while @pos < @bytesize
          k = read_short_string
          v = read_field
          return true if yield(k, v)
        end
        false
      end

      def all?(& : String, Field -> _) : Bool
        @pos = 0
        while @pos < @bytesize
          k = read_short_string
          v = read_field
          return false unless yield(k, v)
        end
        true
      end

      def empty?
        @bytesize.zero?
      end

      def []=(key : String, value : Field)
        check_writeable
        delete(key)
        write_short_string(key)
        write_field(value)
        value
      end

      def to_h
        @pos = 0
        h = Hash(String, Field).new
        while @pos < @bytesize
          k = read_short_string
          h[k] = read_field(table_to_h: true)
        end
        h
      end

      def to_json(json : JSON::Builder)
        json.object do
          @pos = 0
          while @pos < @bytesize
            key = read_short_string
            value = read_field
            json.field key, value
          end
        end
      end

      def inspect(io)
        io << {{@type.name.id.stringify}} << '('
        first = true
        @pos = 0
        while @pos < @bytesize
          io << ", " unless first
          io << '@' << read_short_string
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
        check_writeable
        @pos = 0
        while @pos < @bytesize
          start_pos = @pos
          if key == read_short_string
            v = read_field
            kv_length = @pos - start_pos
            (@buffer + start_pos).move_from(@buffer + @pos, @bytesize - @pos)
            @bytesize -= kv_length
            return v
          else
            skip_field
          end
        end
        nil
      end

      @[Deprecated("key must be String")]
      def delete(key)
        delete(key.to_s)
      end

      def to_io(io, format) : Nil
        io.write_bytes(@bytesize.to_u32, format)
        io.write to_slice
      end

      def self.from_bytes(bytes, format = BYTEFORMAT) : self
        size = format.decode(UInt32, bytes[0, 4])
        if bytes.read_only?
          slice = Bytes.new(bytes.to_unsafe + 4, size, read_only: true)
        else
          slice = Bytes.new(size, read_only: false)
          slice.copy_from(bytes.to_unsafe + 4, size)
        end
        new(slice)
      end

      def self.from_io(io, format, size : UInt32? = nil) : self
        size ||= UInt32.from_io(io, format)
        case io
        when IO::Memory
          if io.@writeable # Need to copy the bytes to prevent modification being mirrored here
            bytes = Bytes.new(size)
            io.read(bytes)
            new(bytes)
          else
            bytes = Bytes.new(io.buffer + io.pos, size, read_only: true)
            io.pos += size
            new(bytes)
          end
        else
          buffer = Bytes.new(size)
          io.read_fully(buffer)
          new(buffer)
        end
      end

      def bytesize
        sizeof(UInt32) + @bytesize
      end

      def to_slice(pos = 0, length = @bytesize - pos)
        Bytes.new(@buffer + pos, length)
      end

      def reject!(& : String, Field -> _) : self
        check_writeable
        @pos = 0
        while @pos < @bytesize
          start_pos = @pos
          key = read_short_string
          value = read_field
          if yield key, value
            kv_length = @pos - start_pos
            (@buffer + start_pos).move_from(@buffer + @pos, @bytesize - @pos)
            @bytesize -= kv_length
            @pos -= kv_length
          end
        end
        self
      end

      def merge!(other : Hash(String, Field) | NamedTuple | self) : self
        check_writeable
        other.each_key { |k| delete(k.to_s) }
        capacity = 0
        other.each { |k, v| capacity += 1 + k.to_s.bytesize + 1 + capacity_required(v) }
        ensure_capacity(capacity)
        other.each do |key, value|
          write_short_string(key.to_s)
          write_field(value)
        end
        self
      end

      private def check_writeable : Nil
        return unless @read_only
        @buffer = to_slice.dup.to_unsafe
        @read_only = false
      end

      private def write_short_string(str : String)
        ensure_capacity(1 + str.bytesize)
        @buffer[@bytesize] = str.bytesize.to_u8; @bytesize += 1
        str.to_slice.copy_to(@buffer + @bytesize, str.bytesize)
        @bytesize += str.bytesize
        str
      end

      private def write_field(value) : Nil
        case value
        when JSON::Any
          write_field(value.raw)
        when Hash, NamedTuple
          write_field(Table.new(value))
        when Int
          ensure_capacity(1 + sizeof_int(value))
          write_prefix(value)
          write_int(value)
        when Float32
          ensure_capacity(1 + sizeof(Float32))
          write_prefix(value)
          BYTEFORMAT.encode(value, Slice.new(@buffer + @bytesize, sizeof(Float32)))
          @bytesize += sizeof(Float32)
        when Float64
          ensure_capacity(1 + sizeof(Float64))
          write_prefix(value)
          BYTEFORMAT.encode(value, Slice.new(@buffer + @bytesize, sizeof(Float64)))
          @bytesize += sizeof(Float64)
        when Bool
          ensure_capacity(1 + sizeof(Bool))
          write_prefix(value)
          @buffer[@bytesize] = value ? 1_u8 : 0_u8
          @bytesize += 1
        when String
          ensure_capacity(1 + sizeof(UInt32) + value.bytesize)
          write_prefix(value)
          write_int(value.bytesize.to_u32)
          value.to_slice.copy_to(@buffer + @bytesize, value.bytesize)
          @bytesize += value.bytesize
        when Bytes
          ensure_capacity(1 + sizeof(UInt32) + value.size)
          write_prefix(value)
          write_int(value.size.to_u32)
          value.copy_to(@buffer + @bytesize, value.size)
          @bytesize += value.size
        when Array
          ensure_capacity(1 + sizeof(UInt32))
          write_prefix(value)
          length_pos = @bytesize
          write_int(0u32) # update size later
          start_pos = @bytesize
          value.each { |v| write_field(v) }
          end_pos = @bytesize
          array_bytesize = end_pos - start_pos
          BYTEFORMAT.encode(array_bytesize.to_u32, Slice.new(@buffer + length_pos, sizeof(UInt32)))
        when Time
          ensure_capacity(1 + sizeof(Int64))
          write_prefix(value)
          write_int(value.to_unix.to_i64)
        when Table
          tbl_slice = value.to_slice
          ensure_capacity(1 + sizeof(UInt32) + tbl_slice.size)
          write_prefix(value)
          write_int(tbl_slice.size.to_u32)
          tbl_slice.copy_to(@buffer + @bytesize, tbl_slice.size)
          @bytesize += tbl_slice.size
        when Nil
          ensure_capacity(1)
          write_prefix(value)
        else raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def ensure_capacity(size : Int)
        required_capacity = @bytesize + size
        if required_capacity > @capacity
          capacity = Math.pw2ceil(required_capacity)
          @buffer = GC.realloc(@buffer, capacity)
          @capacity = capacity
        end
      end

      private def write_prefix(value)
        @buffer[@bytesize] = prefix(value).ord.to_u8
        @bytesize += 1
      end

      private def prefix(value) : Char
        case value
        when Int8    then 'b'
        when UInt8   then 'B'
        when Int16   then 's'
        when UInt16  then 'u'
        when Int32   then 'I'
        when UInt32  then 'i'
        when Int64   then 'l'
        when Float32 then 'f'
        when Float64 then 'd'
        when Bool    then 't'
        when String  then 'S'
        when Bytes   then 'x'
        when Array   then 'A'
        when Time    then 'T'
        when Table   then 'F'
        when Nil     then 'V'
        else              raise "Unexpected field type #{value.class}"
        end
      end

      private def write_int(value) : Nil
        BYTEFORMAT.encode(value, Slice.new(@buffer + @bytesize, sizeof_int(value)))
        @bytesize += sizeof_int(value)
      end

      private def sizeof_int(value) : Int32
        {% begin %}
          case value
          {% for type, i in %w[Int8 UInt8 Int16 UInt16 Int32 UInt32 Int64 UInt64] %}
            {% bytesize = 2 ** (i // 2) %}
            when {{type.id}} then {{ bytesize }}
          {% end %}
          else raise "unexpected value type #{value.class}"
          end
        {% end %}
      end

      private def skip_field : Nil
        type = to_slice[@pos]; @pos += 1
        @pos += sizeof_field_type(type)
      end

      private def sizeof_field_type(type) : Int32
        case type
        when 't' then sizeof(UInt8)
        when 'b' then sizeof(Int8)
        when 'B' then sizeof(UInt8)
        when 's' then sizeof(Int16)
        when 'u' then sizeof(UInt16)
        when 'I' then sizeof(Int32)
        when 'i' then sizeof(UInt32)
        when 'l' then sizeof(Int64)
        when 'f' then sizeof(Float32)
        when 'd' then sizeof(Float64)
        when 'D' then 1 + sizeof(Int32)
        when 'T' then sizeof(Int64)
        when 'S' then 4 + BYTEFORMAT.decode(UInt32, to_slice(@pos))
        when 'x' then 4 + BYTEFORMAT.decode(UInt32, to_slice(@pos))
        when 'A' then 4 + BYTEFORMAT.decode(UInt32, to_slice(@pos))
        when 'F' then 4 + BYTEFORMAT.decode(UInt32, to_slice(@pos))
        when 'V' then 0
        else          raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def capacity_required(value) : Int32
        case value
        when Hash
          sizeof(UInt32) + value.each.sum { |k, v| 1 + k.bytesize + 1 + capacity_required(v) }
        when NamedTuple
          required = sizeof(UInt32)
          value.each { |k, v| required += 1 + k.to_s.bytesize + 1 + capacity_required(v) }
          required
        when JSON::Any then capacity_required(value.raw)
        when Int       then sizeof_int(value)
        when Float32   then sizeof(Float32)
        when Float64   then sizeof(Float64)
        when Bool      then sizeof(Bool)
        when String    then sizeof(UInt32) + value.bytesize
        when Bytes     then sizeof(UInt32) + value.size
        when Array     then sizeof(UInt32) + value.sum { |v| 1 + capacity_required(v) }
        when Time      then sizeof(Int64)
        when Table     then value.bytesize
        when Nil       then 0
        else                raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def skip_short_string
        @pos += 1 + to_slice[@pos]
      end

      private def read_short_string
        slice = to_slice(@pos)
        sz = slice[0]
        str = String.new(slice[1, sz])
        @pos += 1 + sz
        str
      end

      private def read_field(table_to_h = false) : Field
        type = to_slice[@pos]; @pos += 1
        value = case type
                when 't' then to_slice[@pos] == 1_u8
                when 'b' then BYTEFORMAT.decode(Int8, to_slice(@pos))
                when 'B' then BYTEFORMAT.decode(UInt8, to_slice(@pos))
                when 's' then BYTEFORMAT.decode(Int16, to_slice(@pos))
                when 'u' then BYTEFORMAT.decode(UInt16, to_slice(@pos))
                when 'I' then BYTEFORMAT.decode(Int32, to_slice(@pos))
                when 'i' then BYTEFORMAT.decode(UInt32, to_slice(@pos))
                when 'l' then BYTEFORMAT.decode(Int64, to_slice(@pos))
                when 'f' then BYTEFORMAT.decode(Float32, to_slice(@pos))
                when 'd' then BYTEFORMAT.decode(Float64, to_slice(@pos))
                when 'S' then read_long_string
                when 'x' then read_slice
                when 'A' then read_array(table_to_h)
                when 'T' then Time.unix(BYTEFORMAT.decode(Int64, to_slice(@pos)))
                when 'F' then t = Table.from_bytes(to_slice(@pos)); table_to_h ? t.to_h : t
                when 'D' then read_decimal
                when 'V' then nil
                else          raise Error.new "Unknown field type '#{type}'"
                end
        @pos += case type
                when 't' then sizeof(Bool)
                when 'b' then sizeof(Int8)
                when 'B' then sizeof(UInt8)
                when 's' then sizeof(Int16)
                when 'u' then sizeof(UInt16)
                when 'I' then sizeof(Int32)
                when 'i' then sizeof(UInt32)
                when 'l' then sizeof(Int64)
                when 'f' then sizeof(Float32)
                when 'd' then sizeof(Float64)
                when 'T' then sizeof(Int64)
                when 'F' then sizeof(UInt32) + BYTEFORMAT.decode(UInt32, to_slice(@pos))
                else          0
                end
        value
      end

      private def read_long_string : String
        slice = to_slice(@pos)
        sz = BYTEFORMAT.decode(UInt32, slice)
        str = String.new(slice[4, sz])
        @pos += 4 + sz
        str
      end

      private def read_decimal : Float64
        scale = to_slice[@pos]
        value = BYTEFORMAT.decode(Int32, to_slice(@pos + 1))
        @pos += 1 + sizeof(Int32)
        value / 10**scale
      end

      private def read_array(table_to_h = false)
        size = BYTEFORMAT.decode(UInt32, to_slice(@pos))
        @pos += sizeof(UInt32)
        end_pos = @pos + size
        a = Array(Field).new
        while @pos < end_pos
          a << read_field(table_to_h)
        end
        a
      end

      private def read_slice : Bytes
        size = BYTEFORMAT.decode(UInt32, to_slice(@pos))
        bytes = to_slice(@pos + 4, size)
        @pos += 4 + size
        bytes
      end
    end
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
