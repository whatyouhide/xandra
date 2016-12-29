Nonterminals nodes node.
Terminals '<' '>' ',' type.
Rootsymbol node.

node -> type : extract_type('$1').
node -> type '<' nodes '>' : [extract_type('$1') | '$3'].

nodes -> node : ['$1'].
nodes -> node ',' nodes : ['$1' | '$3'].

Erlang code.

extract_type({type, _Line, Type}) -> Type.
