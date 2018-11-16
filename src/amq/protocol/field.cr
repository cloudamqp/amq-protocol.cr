module AMQ
  module Protocol
    alias Field =
      Nil |
      Bool |
      Int8 |
      UInt8 |
      Int16 |
      UInt16 |
      Int32 |
      UInt32 |
      Int64 |
      Float32 |
      Float64 |
      String |
      Time |
      Hash(String, Field) |
      Array(Field) |
      Bytes |
      Array(Hash(String, Field))

    # https://github.com/crystal-lang/crystal/issues/4885#issuecomment-325109328
    def self.cast_to_field(x :  Array)
      return x.map { |e| cast_to_field(e).as(Field) }.as(Field)
    end

    def self.cast_to_field(x : Hash)
      h = Hash(String, Field).new
      x.each do |(k, v)|
        h[k] = cast_to_field(v).as(Field)
      end
      h.as(Field)
    end

    def self.cast_to_field(x)
      x.try &.raw.as(Field)
    end
  end
end
