class SSDB
  T_BOOL    = ->r { r == "1" }
  T_INT     = ->r { r.to_i }
  T_CINT    = ->r { r.to_i if r }
  T_VBOOL   = ->r { r.each_slice(2).map {|_, v| v == "1" }}
  T_VINT    = ->r { r.each_slice(2).map {|_, v| v.to_i }}
  T_STRSTR  = ->r { r.each_slice(2).to_a }
  T_STRINT  = ->r { r.each_slice(2).map {|v, s| [v, s.to_i] } }
  T_MAPINT  = ->r,n { h = {}; r.each_slice(2) {|k, v| h[k] = v }; n.map {|k| h[k].to_i } }
  T_MAPSTR  = ->r,n { h = {}; r.each_slice(2) {|k, v| h[k] = v }; n.map {|k| h[k] } }
  T_HASHSTR = ->r { h = {}; r.each_slice(2) {|k, v| h[k] = v }; h }
  T_HASHINT = ->r { h = {}; r.each_slice(2) {|k, v| h[k] = v.to_i }; h }
  BLANK     = "".freeze

end