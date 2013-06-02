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

  DB_STATS  = ["compactions", "level", "size", "time", "read", "written"].freeze
  T_INFO    = ->rows {
    res = {}
    rows.shift # skip first
    rows.each_slice(2) do |key, val|
      res[key] = case key
      when "leveldb.stats"
        stats = {}
        val.lines.to_a.last.strip.split(/\s+/).each_with_index do |v, i|
          stats[DB_STATS[i]] = v.to_i
        end
        stats
      when /^cmd\./
        val.split("\t").inject({}) do |stats, i|
          k, v = i.split(": ", 2)
          stats.update k => v.to_i
        end
      else
        val.to_i
      end
    end
    res
  }
end