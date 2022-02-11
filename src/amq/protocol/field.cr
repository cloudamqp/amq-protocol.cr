require "json/any"
require "./table"

module AMQ
  module Protocol
    alias Field = Nil |
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
                  Table |
                  Hash(String, Field) |
                  NamedTuple(key: String) |
                  NamedTuple(key: Symbol) |
                  Array(Field) |
                  Bytes |
                  Array(Table) |
                  JSON::Any
  end
end
