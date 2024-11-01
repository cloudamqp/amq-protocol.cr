alias UInt = UInt32
alias UShort = UInt16
alias Milliseconds = UInt

struct Symbol
  # TODO
end

alias IETFLanguageTag = Symbol

alias Value = UInt | UShort | Milliseconds | Symbol | Array(Value) |
              IETFLanguageTag | Map(Symbol, Value)

# A polymorphic mapping from distinct keys to values
struct Map(K, V)
  # TODO
end

alias Fields = Map(Symbol, Value)

# A sequence of polymorphic values
alias List = Array(Value)
