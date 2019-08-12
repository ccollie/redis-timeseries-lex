-- UNIVARIATE TIMESERIES IN REDIS
--
-- Stand-alone Lua script for managing an univariate timeseries in Redis based on lexicographically sorted sets
--
-- A timeseries is an
--  1) ordered (with respect to timestamps)
--  2) unique (each timestamp is unique within the timeseries)
--  3) associative (it associate a timestamp with a value) container.
-- Commands are implemented in the *Timeseries* table. To
-- execute a command use EVALSHA as follows
--
--  EVALSHA sha1 1 key add 100000 key value
--  EVALSHA sha1 1 key range 100000 100500 0 25

-- Originally based on https://searchcode.com/codesearch/view/15359364/#

local _NAME = 'timeseries-lex.lua'
local _VERSION = '0.0.1'
local _DESCRIPTION = 'A library for simple timeseries handling in Redis'
local _COPYRIGHT = '2019 Clayton Collie, Guanima Tech'

local function ts_debug(msg)
    redis.call('rpush', 'ts-debug', msg)
end

----- DEBUG HELPERS ---

function table.val_to_str (v)
    if "string" == type(v) then
        v = string.gsub(v, "\n", "\\n")
        if string.match(string.gsub(v, "[^'\"]", ""), '^"+$') then
            return "'" .. v .. "'"
        end
        return '"' .. string.gsub(v, '"', '\\"') .. '"'
    else
        return "table" == type(v) and table.tostring(v) or
                tostring(v)
    end
end

function table.key_to_str (k)
    if "string" == type(k) and string.match(k, "^[_%a][_%a%d]*$") then
        return k
    else
        return "[" .. table.val_to_str(k) .. "]"
    end
end

function table.tostring(tbl)
    local result, done = {}, {}
    for k, v in ipairs(tbl) do
        table.insert(result, table.val_to_str(v))
        done[k] = true
    end
    for k, v in pairs(tbl) do
        if not done[k] then
            table.insert(result,
                    table.key_to_str(k) .. "=" .. table.val_to_str(v))
        end
    end
    return "{" .. table.concat(result, ",") .. "}"
end

--- STATS ------

local stats = {}

-- Get the mean value of a table

function stats.mean(t)
    local sum = 0
    local count = 0

    for k, v in pairs(t) do
        if type(v) == 'number' then
            sum = sum + v
            count = count + 1
        end
    end

    return (sum / count)
end

function stats.basic(t)
    local count, sum = 0, 0
    local max = -math.huge
    local min = math.huge
    local vk, mean = 0, 0
    local std = 0
    local math_max = math.max
    local math_min = math.min
    local sqrt = math.sqrt

    for _, v in pairs(t) do
        local val = tonumber(v)
        if val ~= nil then
            local oldmean = mean
            count = count + 1
            sum = sum + val
            max = math_max(max, val)
            min = math_min(min, val)
            mean = sum / count
            vk = vk + (val - mean) * (val - oldmean)
            std = sqrt(vk / (count - 1))
        end
    end
    return {
        count = count,
        sum = sum,
        min = min,
        max = max,
        mean = mean,
        std = std
    }
end


--- UTILS ------

local SEPARATOR = '|'
local IDENTIFIER_PATTERN = "[%a_]+[%a%d_]*"
local ID_CAPTURE_PATTERN = '(' .. IDENTIFIER_PATTERN .. ')'

local function is_possibly_number(val)
    local res = tonumber(val)
    local is_num = (res ~= nil)
    if is_num then
        val = res
    end
    return is_num, val
end

local function parse_input(val)
    local is_num, num = is_possibly_number(val)
    if (is_num) then
        if (math.floor(num) ~= num) then
            return val
        end
        return num
    end
    if (val == 'true') then
        return true
    elseif (val == 'false') then
        return false
    end
    return val
end

local function possibly_convert_float(val)
    if (type(val) == 'number' and (math.floor(val) ~= val)) then
        return tostring(val)
    end
    return val
end

-- for return to redis
-- not very sophisticated since our values are simple
local function to_bulk_reply(val)
    local type = type(val)
    if type == 'number' then
        -- handle floats
        if (math.floor(val) ~= val) then
            return tostring(val)
        end
        return val
    elseif type == "table" then
        local data = {}
        -- check if is_array
        if val[1] ~= nil then
            for j, xval in ipairs(val) do
                data[j] = to_bulk_reply(xval)
            end
            return data
        end
        -- associative
        local i = 1
        for k, v in pairs(val) do
            data[i] = k
            data[i + 1] = to_bulk_reply(v)
            i = i + 2
        end
        return data
    elseif type == "nil" then
        return "nil"
    end
    return val
end

-- raw value should be a kv table [name, value, name, value ...]
-- convert to an associative array
local function to_hash(value)
    local len, result = #value, {}
    for k = 1, len, 2 do
        result[value[k]] = value[k + 1]
    end
    return result
end

local function from_hash(table)
    local i, data = 1, {}
    for k, v in pairs(table) do
        data[i] = k
        data[i + 1] = v
        i = i + 2
    end
    return data
end

local function table_keys(table)
    local i, data = 1, {}
    for k, _ in pairs(table) do
        data[i] = k
        i = i + 1
    end
    return data
end

-- value is raw value from zrangebylex
-- only call this if (options.labels or options.redacted)
local function pick(value, options)
    local hash = {}
    local key, val, valid
    local i, len = 1, #value

    for k = 1, len, 2 do
        key = value[k]
        val = value[k + 1]
        valid = true
        if options.labels then
            valid = options.labels[key]
        elseif options.redacted then
            valid = (not options.redacted[key])
        end
        if valid then
            hash[i] = key
            hash[i + 1] = val
            i = i + 2
        end
    end
    return hash
end

local function split(source, sep)
    local start, ending = string.find(source, sep or SEPARATOR, 1, true)
    local timestamp = source:sub(1, start - 1)
    local value = source:sub(ending + 1)
    return tonumber(timestamp), value
end

local function encode_value(ts, data)
    return tostring(ts) .. SEPARATOR .. cmsgpack.pack(data)
end

local function decode_value(raw_value)
    local ts, block = split(raw_value)
    return ts, cmsgpack.unpack(block)
end

local function store_value(key, timestamp, value, is_hash)
    if (is_hash) then
        value = from_hash(value)
    end
    local val = encode_value(timestamp, value)
    redis.call('zadd', key, 0, val)
end

local function format_result(data, format)
    if (format == nil) then
        return to_bulk_reply(data)
    elseif (format == 'json') then
        return cjson.encode(data)
    elseif (format == 'msgpack') then
        return cmsgpack.pack(data)
    end
end
--- PARAMETER PARSING --------

local function parse_range_value(candidate, name)
    assert(candidate, 'value expected for ' .. name)
    local first = candidate:sub(1, 1)
    if (first == '*') then
        local val = redis.call('TIME')
        return tonumber(val[1]), false
    end
    if (first == '+') or (first == '-') or (first == '[') or (first == '(') then
        return candidate, true
    else
        local value = tonumber(candidate)
        if (value == nil) then
            error('number expected for ' .. name)
        end
        return value, false
    end
end

local function parse_range_min_max(result, timestamp1, timestamp2)
    local min_val, min_special = parse_range_value(timestamp1, 'min')
    local max_val, max_special = parse_range_value(timestamp2, 'max')

    local min, max = min_val, max_val
    if not min_special then
        min = '[' .. min_val
    end
    if not max_special then
        --- add fudge factor to capture values at the edge of the range
        local fudge = 1

        if not min_special then
            -- if we're reversed, then reverse fudge
            if max_val < min_val then
                fudge = -1
            end
        end
        max = '(' .. tostring(max_val + fudge)
    end

    result[#result + 1] = min
    result[#result + 1] = max
end

local function get_key_val_varargs(method, ...)
    local arg = { ... }
    local n = #arg

    assert(n, 'No values specified for  ' .. method .. '.')
    assert(math.mod(n, 2) == 0, 'Invalid args to ' .. method .. '. Number of arguments must be even')
    return arg
end


-- Source https://help.interfaceware.com/kb/parsing-csv-files
local function parse_list (line, sep)
    local res = {}
    local pos = 1
    sep = sep or ','
    while true do
        local c = string.sub(line, pos, pos)
        if (c == "") then
            break
        end
        local posn = pos
        local ctest = string.sub(line, pos, pos)
        while ctest == ' ' do
            -- handle space(s) at the start of the line (with quoted values)
            posn = posn + 1
            ctest = string.sub(line, posn, posn)
            if ctest == '"' then
                pos = posn
                c = ctest
            end
        end
        if (c == '"') then
            -- quoted value (ignore separator within)
            local txt = ""
            repeat
                local startp, endp = string.find(line, '^%b""', pos)
                txt = txt .. string.sub(line, startp + 1, endp - 1)
                pos = endp + 1
                c = string.sub(line, pos, pos)
                if (c == '"') then
                    txt = txt .. '"'
                    -- check first char AFTER quoted string, if it is another
                    -- quoted string without separator, then append it
                    -- this is the way to "escape" the quote char in a quote. example:
                    --   value1,"blub""blip""boing",value3  will result in blub"blip"boing  for the middle
                elseif c == ' ' then
                    -- handle space(s) before the delimiter (with quoted values)
                    while c == ' ' do
                        pos = pos + 1
                        c = string.sub(line, pos, pos)
                    end
                end
            until (c ~= '"')
            table.insert(res, txt)
            -- trace(c,pos,i)
            if not (c == sep or c == "") then
                error("ERROR: Invalid field - near character " .. pos .. " in the item list: \n" .. line, 3)
            end
            pos = pos + 1
            posn = pos
            ctest = string.sub(line, pos, pos)
            -- trace(ctest)
            while ctest == ' ' do
                -- handle space(s) after the delimiter (with quoted values)
                posn = posn + 1
                ctest = string.sub(line, posn, posn)
                if ctest == '"' then
                    pos = posn
                    c = ctest
                end
            end
        else
            -- no quotes used, just look for the first separator
            local startp, endp = string.find(line, sep, pos)
            if (startp) then
                table.insert(res, string.sub(line, pos, startp - 1))
                pos = endp + 1
            else
                -- no separator found -> use rest of string and terminate
                table.insert(res, string.sub(line, pos))
                break
            end
        end
    end
    return res
end

--- Parse a filter condition and return a function implementing
--- the corresponding filter predicate
local function parse_filter_condition(exp)

    local function compare(_field, _op, _val)
        local num = tonumber(_val)
        local is_numeric = num ~= nil
        if (is_numeric) then
            _val = num
        else
            _val = parse_input(_val)
        end
        local val_type = type(_val)
        return function(v)
            local val_type = val_type
            local val = _val
            local is_numeric = is_numeric
            local op = _op
            local field = _field

            local to_compare = v[field]

            if (val_type ~= type(to_compare)) then
                if is_numeric then
                    to_compare = tonumber(to_compare)
                else
                    -- convert all to string
                    val = tostring(val)
                    to_compare = tostring(to_compare)
                end
            end
            -- ts_debug('filtering. v = ' .. table.tostring(v) .. ' field = ' .. field .. ', comp = ' .. tostring(to_compare) .. ' ' .. op .. ' ' .. tostring(val))
            if (op == 'eq') then
                return val == to_compare
            elseif (op == 'ne') then
                return val ~= to_compare
            else
                if (val == nil) or (to_compare == nil) then
                    return false
                end
                if (op == 'gt') then
                    return to_compare > val
                elseif (op == 'lt') then
                    return to_compare < val
                elseif (op == 'gte') then
                    return to_compare >= val
                elseif (op == 'lte') then
                    return to_compare <= val
                end
            end
            return false
        end
    end

    local function contains(_field, _matches)
        return function(v)
            local matches = _matches
            local field = _field
            return matches[tostring(v[field])] ~= nil
        end
    end

    local function not_contains(_field, _matches)
        return function(v)
            local matches = _matches
            local field = _field
            return matches[tostring(v[field])] == nil
        end
    end

    local function match_contains(cond)
        local values, _, field
        local ops = { '!=', '=' }
        local contains_funcs = { not_contains, contains }

        for i, op in ipairs(ops) do
            local pattern = ID_CAPTURE_PATTERN .. op .. '(%b())'
            _, _, field, values = string.find(cond, pattern)
            if field and values then
                -- remove parens
                values = values:sub(2, values:len() - 1)

                local matches = parse_list(values, ',')
                if #matches == 0 then
                    error('No values found for contains match')
                end
                -- transform indexed array to an associative hash for faster comparisons
                local temp = {}
                for _, val in ipairs(matches) do
                    temp[tostring(val)] = 1
                end
                matches = temp
                return contains_funcs[i](field, matches)
            end
        end

        return nil
    end

    local function match_ops(cond)
        local pattern, val, field, _
        local ops = { '!=', '<=', '>=', '=', '<', '>' }
        local op_names = { 'ne', 'lte', 'gte', 'eq', 'lt', 'gt' }

        for i, op in ipairs(ops) do
            pattern = ID_CAPTURE_PATTERN .. op .. '(.+)'
            _, _, field, val = string.find(cond, pattern)
            if (field and val) then
                return compare(field, op_names[i], val)
            end
        end

        return nil
    end

    local p = assert(match_contains(exp) or match_ops(exp), 'FILTER: unable to parse expression : ' .. exp)

    return p
end

local AGGREGATION_TYPES = {
    count = 1,
    sum = 1,
    avg = 1,
    min = 1,
    max = 1,
    first = 1,
    last = 1,
    range = 1,
    data = 1,
    distinct = 1,
    stats = 1,
    count_distinct = 1,
    rate = 1
}

local AGGREGATION_RETURNS_TABLE = {
    data = 1,
    stats = 1,
    distinct = 1,
    count_distinct = 1
}

local ALL_OPTIONS = {
    LIMIT = 1,
    AGGREGATION = 1,
    FILTER = 1,
    LABELS = 1,
    REDACT = 1,
    FORMAT = 1,
    STORAGE = 1
}

local PARAMETER_OPTIONS = {
    LIMIT = 1,
    AGGREGATION = 1,
    FILTER = 1,
    LABELS = 1,
    REDACT = 1,
    FORMAT = 1
}

local COPY_OPTIONS = {
    LIMIT = 1,
    AGGREGATION = 1,
    FILTER = 1,
    LABELS = 1,
    REDACT = 1,
    STORAGE = 1
}

local FORMAT_VALUES = {
    json = 1,
    msgpack = 1
}

local STORAGE_VALUES = {
    timeseries = 1,
    hash = 1
}

-- Returns a predicate function that matches
-- *all* of the given predicate functions.
local function join_AND(predicates)
    return function(s)
        for _, func in ipairs(predicates) do
            if not func(s) then
                return false
            end
        end
        return true
    end
end

-- Returns a predicate function that matches
-- *any* of the given predicate functions.
local function join_OR(predicates)
    return function(s)
        for _, func in ipairs(predicates) do
            if func(s) then
                return true
            end
        end
        return false
    end
end

local function parse_filter(args, i)
    local predicate, expr
    local predicates = {}
    local count = 0
    local u_expr
    local parse_condition = parse_filter_condition
    local len = #args
    local join_funcs = {
        OR = join_OR,
        AND = join_AND
    }

    local function parse_join(chain, arg, op, j)
        local exp = op;
        while (exp == op) do
            j = j + 1
            chain[#chain + 1] = parse_condition(arg[j])
            j = j + 1
            if (j >= len) then
                break
            end
            exp = string.upper(arg[j])
        end
        return j
    end

    local chain
    while i <= len do
        expr = args[i]
        u_expr = string.upper(expr)
        if (ALL_OPTIONS[u_expr]) then
            break
        end

        predicate = parse_condition(expr)
        i = i + 1
        u_expr = string.upper(args[i] or '')
        while (u_expr == 'AND') or (u_expr == 'OR') do
            chain = { predicate }
            i = parse_join(chain, args, u_expr, i)
            predicate = join_funcs[u_expr](chain)
            if (i >= len) then
                break
            end
            u_expr = string.upper(args[i])
        end
        count = count + 1
        predicates[count] = predicate
    end

    assert(count > 0, 'FILTER: at least one condition must be specified ')
    -- construct final predicate
    -- Optimize for common case (1 condition)
    if (count == 1) then
        return predicates[1], i
    else
        return join_AND(predicates), i
    end
end

local function parse_range_params(valid_options, min, max, ...)
    local PARAMETER_OPTIONS = PARAMETER_OPTIONS
    local fetch_params = {}
    parse_range_min_max(fetch_params, min, max)

    local result = {
        min = fetch_params[1],
        max = fetch_params[2]
    }

    valid_options = valid_options or PARAMETER_OPTIONS

    local arg = { ... }
    local i = 1

    --- ts_debug('args = ' .. table.tostring(arg))
    --- [LIMIT count] or
    --- [AGGREGATION bucketWidth aggregateType]
    --- [FILTER key=value, ...]
    --- [LABELS name1, name2 ....]
    while i < #arg do
        local option_name = assert(arg[i], 'range: no option specified')
        option_name = string.upper(option_name)

        if (not valid_options[option_name]) then
            local j = 0
            local str = ''
            for k, _ in pairs(valid_options) do
                if str:len() > 0 then
                    str = str .. ', '
                end
                str = str .. k
                j = j + 1
            end
            error('Invalid option "' .. option_name .. '". Expected one of ' .. str)
        end

        i = i + 1
        if (option_name == 'LIMIT') then
            assert(not result.limit, 'A value for limit has already been set')

            result.limit = {
                offset = 0
            }
            -- we should have offset, count
            result.limit.offset = assert(tonumber(arg[i]), 'LIMIT: offset value must be a number')
            assert(result.limit.offset >= 0, "LIMIT: offset must be 0 or positive")

            result.limit.count = assert(tonumber(arg[i + 1]), 'LIMIT: count value must be a number')

            i = i + 2
        elseif (option_name == 'AGGREGATION') then
            assert(not result.aggregate, 'A value for aggregate has already been set')

            local bucketSize = arg[i]
            result.labels = {}
            result.aggregate = {
                timeBucket = assert(tonumber(bucketSize), 'AGGREGATE: timeBucket must be a number. Got "' .. bucketSize .. '"'),
                fields = {}
            }
            i = i + 1
            local agg, field
            while i <= #arg do
                agg = arg[i]
                if (ALL_OPTIONS[string.upper(agg)]) then
                    break
                end
                agg = assert(string.lower(agg), 'missing value for aggregate type')
                assert(AGGREGATION_TYPES[agg], 'invalid aggregation type : "' .. agg ..'"')
                i = i + 1
                field = assert(arg[i], 'missing field in aggregate')
                table.insert(result.aggregate.fields, { field, agg })
                result.labels[field] = 1
                i = i + 1
            end
            -- make sure some fields were specified
            assert(#result.aggregate.fields, 'No fields specified for aggregation')
        elseif (option_name == "LABELS") then
            assert(not result.labels, 'LABELS option already specified')
            assert(not result.redacted, 'Either specify REDACT or LABELS, but not both')
            result.labels = {}
            while i <= #arg do
                local key = arg[i]
                if (ALL_OPTIONS[string.upper(key)]) then
                    break
                end
                result.labels[key] = 1
                i = i + 1
            end
        elseif (option_name == 'REDACT') then
            assert(not result.redacted, 'REDACT option already specified')
            assert(not result.labels, 'Either specify REDACT or LABELS, but not both')
            result.redacted = {}
            while i <= #arg do
                local key = arg[i]
                if (ALL_OPTIONS[string.upper(key)]) then
                    break
                end
                result.redacted[key] = 1
                i = i + 1
            end
        elseif (option_name == 'FILTER') then
            assert(not result.filter, 'FILTER conditions already set')
            local predicate

            predicate, i = parse_filter(arg, i)

            result.filter = predicate

        elseif (option_name == 'FORMAT') then
            assert(not result.format, 'FORMAT already set')
            local format = string.lower(arg[i] or '')
            assert(FORMAT_VALUES[format], 'FORMAT: Expecting "json" or "msgpack"')
            result.format = format
            i = i + 1
        elseif (option_name == 'STORAGE') then
            assert(not result.storage, 'STORAGE already set')
            local storage = string.lower(arg[i] or '')
            assert(STORAGE_VALUES[storage], 'STORAGE: Expecting "timeseries" or "hash", got "' .. storage .. '"')
            result.storage = storage
            i = i + 1
        end
    end

    return result
end

local function process_range(range, options)
    local result = {}
    local hash
    local valid
    local pick = pick
    local decode = decode_value
    local to_hash = to_hash

    options = options or {}
    local should_pick = options.labels or options.redacted
    local filter = options.filter

    local i = 1
    for _, value in ipairs(range) do
        local ts, val = decode(value)
        valid = true
        if filter then
            hash = to_hash(val)
            valid = filter(hash)
        end
        if valid then
            if should_pick then
                val = pick(val, options)
            end
            if #val then
                result[i] = { ts, val }
                i = i + 1
            end
        end
    end
    return result
end

local function get_single_value(key, timestamp, options, name)
    options = options or {}
    timestamp = assert(tonumber(timestamp), (name or 'get_single_value') .. ': timestamp must be a number')
    local min = '[' .. tostring(timestamp) .. SEPARATOR
    local max = '(' .. tostring(timestamp + 1) .. SEPARATOR
    local ra = redis.call('zrangebylex', key, min, max, 'limit', 0, 2)
    if ra ~= nil and #ra == 1 then
        local raw_value = ra[1]
        local ts, value = decode_value(raw_value)
        value = ((options.labels or options.redacted) and pick(value, options)) or value
        return {
            ts = ts,
            value = value,
            raw_value = raw_value
        }
    elseif #ra > 1 then
        error('Critical error in timeseries.' .. name .. ' : multiple values for a timestamp')
    end
    return nil
end

local AGGR_ITERATION_FUNCS = {
    count = function(result, key, val)
        result[key] = tonumber(result[key] or 0) + 1
    end,
    sum = function(result, key, val)
        val = tonumber(val) or 0
        result[key] = tonumber(result[key] or 0) + val
    end,
    avg = function(result, key, val)
        val = tonumber(val)
        if val ~= nil then
            result[key] = result[key] or {}
            table.insert(result[key], val)
        end
    end,
    stats = function(result, key, val)
        val = tonumber(val)
        if val ~= nil then
            result[key] = result[key] or {}
            table.insert(result[key], val)
        end
    end,
    rate = function(result, key, val)
        result[key] = tonumber(result[key] or 0) + 1
    end,
    min = function(result, key, val)
        local is_number
        local current = result[key]
        if (val ~= nil) then
            is_number, val = is_possibly_number(val)
            if (is_number) then
                result[key] = math.min(current or math.huge, val)
            else
                current = current or ''
                if (val < current) then
                    result[key] = val
                else
                    result[key] = current
                end
            end
        end
    end,
    max = function(result, key, val)
        local is_number
        local current = result[key]
        if (val ~= nil) then
            is_number, val = is_possibly_number(val)
            if (is_number) then
                result[key] = math.max(current or -math.huge, val)
            else
                current = current or ''
                if (val > current) then
                    result[key] = val
                else
                    result[key] = current
                end
            end
        end
    end,
    range = function(result, key, val)
        val = tonumber(val)
        if val ~= nil then
            result[key] = result[key] or { min = math.huge, max = -math.huge }
            local min_max = result[key]
            min_max.min = math.min(min_max.min, val)
            min_max.max = math.max(min_max.max, val)
        end
    end,
    first = function(result, key, val)
        if (val ~= nil) then
            if (result[key] == nil) then
                result[key] = val
            end
        end
    end,
    last = function(result, key, val)
        result[key] = val
    end,
    distinct = function(result, key, val)
        --- distinct values
        result[key] = result[key] or {}
        result[key][tostring(val)] = 1
    end,
    count_distinct = function(result, key, val)
        --- count distinct values
        result[key] = result[key] or {}
        local slot = result[key]
        local val_key = tostring(val)
        slot[val_key] = tonumber(slot[val_key] or 0) + 1
    end,
    data = function(result, key, val)
        result[key] = result[key] or {}
        table.insert(result[key], val)
    end
}

local AGGR_FINALIZE_FUNCS = {
    default = function(result)
        return possibly_convert_float(result)
    end,
    avg = function(result)
        for bucket, data in pairs(result) do
            result[bucket] = possibly_convert_float( stats.mean(data) )
        end
        return result
    end,
    stats = function(result)
        for bucket, data in pairs(result) do
            local stat = stats.basic(data)
            local temp = {}
            for k, v in pairs(stat) do
                temp[#temp + 1] = k
                temp[#temp + 1] = possibly_convert_float(v)
            end
            result[bucket] = temp
        end
        return result
    end,
    range = function(result)
        for bucket, min_max in pairs(result) do
            if (min_max ~= nil) then
                result[bucket] = possibly_convert_float(min_max.max - min_max.min)
            else
                result[bucket] = false  -- how do we return nil back to redis ????
            end
        end
        return result
    end,
    distinct = function(result)
        for bucket, values in pairs(result) do
            result[bucket] = from_hash(values)
        end
        return result
    end,
    rate = function(result, timeBucket)
        for bucket, count in pairs(result) do
            result[bucket] = tostring(count / timeBucket)
        end
        return result
    end
}

local function aggregate(range, aggregationType, timeBucket)
    local result = {}
    local ts, key, val

    local iterate = assert(AGGR_ITERATION_FUNCS[aggregationType], 'invalid aggregate type "' .. tostring(aggregationType) .. '"')
    local finalize = AGGR_FINALIZE_FUNCS[aggregationType] or AGGR_FINALIZE_FUNCS.default

    for _, kv in ipairs(range) do
        ts = kv[1] - (kv[1] % timeBucket)
        val = kv[2]
        key = tostring(ts)
        iterate(result, key, val)
    end

    return finalize(result, timeBucket)
end

local function base_range(cmd, key, params)
    local fetch_params = { key, params.min, params.max }
    if (params.limit) then
        fetch_params[#fetch_params + 1] = 'LIMIT'
        fetch_params[#fetch_params + 1] = params.limit.offset
        fetch_params[#fetch_params + 1] = params.limit.count
    end
    return redis.call(cmd, unpack(fetch_params))
end

-- COMMANDS TABLE
local Timeseries = {
}

Timeseries.__index = Timeseries;

local function parse_timestamp(ts)
    if (ts == '*') then
        local val = redis.call('TIME')
        return val[1]
    end
    return ts
end

-- Add timestamp-value pairs to the Timeseries
function Timeseries.add(key, timestamp, ...)
    timestamp = assert(parse_timestamp(timestamp), 'timestamp should be a number')
    local values = get_key_val_varargs('add', ...)
    local parse_value = parse_input
    local len = #values

    for i = 1, len, 2 do
        values[i + 1] = parse_value(values[i + 1])
    end
    store_value(key, timestamp, values, false)
    return timestamp
end

function Timeseries.del(key, ...)
    local args = { ... }
    assert(#args > 0, "At least one item must be specified for del")

    local values = {}
    local min, max, entries
    local count = 0
    for _, timestamp in ipairs(args) do
        timestamp = assert(parse_timestamp(timestamp), 'del: timestamp must be a number')
        min = '[' .. tostring(timestamp) .. SEPARATOR
        max = '(' .. tostring(timestamp + 1) .. SEPARATOR
        entries = redis.call('zrangebylex', key, min, max)
        if entries and #entries then
            for _, raw_value in ipairs(entries) do
                count = count + 1
                values[count] = raw_value
            end
        end
    end

    if count == 0 then
        return 0
    end

    return redis.call('zrem', key, unpack(values))
end

function Timeseries.size(key)
    return redis.call('zcard', key)
end

-- Count the number of elements between *min* and *max*
function Timeseries.count(key, min, max, ...)
    local params = parse_range_params({ FILTER = 1 }, min, max, ...)
    if (params.filter == nil) then
        return redis.call('zlexcount', key, params.min, params.max)
    end
    local data = base_range('zrangebylex', key, params)
    if data and #data > 0 then
        local range = process_range(data, params)
        return #range
    end
    return 0
end

-- Check if *timestamp* exists in the timeseries
function Timeseries.exists(key, timestamp)
    local value = get_single_value(key, parse_timestamp(timestamp), 'exists')
    if value ~= nil then
        return 1
    else
        return 0
    end
end

function Timeseries.span(key)
    local count = redis.call('zcard', key)
    if count == 0 then
        return {}
    end
    local firstTs = redis.call('zrangebylex', key, '-', '+', 'limit', 0, 1)
    local lastTs = redis.call('zrevrangebylex', key, '+', '-', 'limit', 0, 1)

    if #firstTs then
        firstTs = split(firstTs[1])
    end

    if #lastTs then
        lastTs = split(lastTs[1])
    end

    return { firstTs, lastTs }
end

function Timeseries._get(remove, key, timestamp, ...)
    local params = parse_range_params({ LABELS = 1, REDACT = 1, FORMAT = 1 }, timestamp, timestamp, ...)
    local entry = get_single_value(key, parse_timestamp(timestamp), params, 'get')
    if entry then
        local result = entry.value
        if (remove) then
            redis.call("zrem", key, entry.raw_value)
        end
        if (params.format == nil) then
            -- to_bulk_reply
            return result
        end
        return format_result(to_hash(result), params.format)
    end
end

-- Get the value associated with *timestamp*
function Timeseries.get(key, timestamp, ...)
    return Timeseries._get(false, key, timestamp, ...)
end

-- Remove and return the value associated with *timestamp*
function Timeseries.pop(key, timestamp, ...)
    return Timeseries._get(true, key, timestamp, ...)
end

-- Set the values of a hash associated with *timestamp*
function Timeseries.set(key, timestamp, ...)
    local current = get_single_value(key, parse_timestamp(timestamp), 'set')
    local hash
    if (current == nil) then
        hash = {}
    else
        hash = to_hash(current.value)
        assert(type(hash) == "table", 'set:. The value at ' .. key .. '(' .. tostring(timestamp) .. ') is not a hash')
    end

    local values = get_key_val_varargs('set', ...)

    local count = 0
    local parse = parse_input
    for i = 1, #values, 2 do
        hash[values[i]] = parse(values[i + 1])
        count = count + 1
    end

    -- remove old value
    if (current ~= nil) then
        redis.call("zrem", key, current.raw_value)
    end

    store_value(key, timestamp, hash, true)
end

-- The list of timestamp-value pairs between *min* and *max* with optional offset and count
function Timeseries.range(key, min, max, ...)
    return Timeseries._range('zrangebylex', key, min, max, ...)
end

-- The descending list of timestamp-value pairs between *timestamp1* and *max* with optional offset and count
function Timeseries.revrange(key, max, min, ...)
    return Timeseries._range('zrevrangebylex', key, min, max, ...)
end

local function remove_values(key, values, filter)

    if values and #values > 0 then

        if (filter == nil) then
            return redis.call('zrem', key, unpack(values))
        end

        local to_delete = {}
        local count = 0

        local to_hash = to_hash
        local decode = decode_value

        for _, value in ipairs(values) do
            local _, val = decode(value)
            local hash = to_hash(val)
            if filter(hash) then
                count = count + 1
                to_delete[count] = value
            end
        end
        if count == 0 then
            return 0
        end
        return redis.call('zrem', key, unpack(to_delete))
    end
    return 0
end

-- Remove a range between *min* and *max*
function Timeseries.remrange(key, min, max, ...)
    local params = parse_range_params({ FILTER = 1, LIMIT = 1 }, min, max, ...)
    if (params.filter == nil) and (params.limit == nil) then
        return redis.call('zremrangebylex', key, params.min, params.max)
    end
    local data = base_range('zrangebylex', key, params)
    return remove_values(key, data, params.filter)
end


-- increment value(s) at key(s)
--- incrby(key, ts, name1, value1, name2, value2, ...)
function Timeseries.incrBy(key, timestamp, ...)
    timestamp = parse_timestamp(timestamp)
    local current = get_single_value(key, timestamp, {}, 'incrBy')

    local hash = {}
    if (current ~= nil) then
        hash = to_hash(current.value)
        assert(type(hash) == "table", 'incrBy. The value at ' .. key .. '(' .. tostring(timestamp) .. ') is not a hash')
    end

    local values = get_key_val_varargs('incrby', ...)

    local len, count = #values, 0
    local result = {}
    for i = 1, len, 2 do
        local name = values[i]
        local increment = tonumber(values[i + 1]) or 0
        hash[name] = tonumber(hash[name] or 0) + increment
        count = count + 1
        result[count] = hash[name]
    end

    -- remove old value
    if (current ~= nil) then
        redis.call("zrem", key, current.raw_value)
    end

    store_value(key, timestamp, hash, true)

    return to_bulk_reply(result)
end

function Timeseries._range(remove, cmd, key, min, max, ...)

    local function handle_aggregation(range, agg_params, format)
        local aggregate = aggregate
        local by_key = {}
        local k
        for _, v in ipairs(range) do
            local ts = v[1]
            local hash = v[2] or {}
            for i = 1, #hash, 2 do
                k = hash[i]
                by_key[k] = by_key[k] or {}
                table.insert(by_key[k], { ts, hash[i + 1] })
            end
        end
        local result = {}
        local bucket_list = {}
        local bucket_hash = {}

        for _, field_info in ipairs(agg_params.fields) do
            local key = field_info[1]
            local agg_type = field_info[2]
            local values = by_key[key]

            if (values and #values > 0) then
                local buckets = aggregate(values, agg_type, agg_params.timeBucket)
                for k, value in pairs(buckets) do
                    result[k] = result[k] or {}
                    result[k][key] = result[k][key] or {}

                    local temp = result[k][key]
                    if (format == 'json') or (format == 'msgpack') then
                        if (agg_type == 'distinct') then
                            local data = {}
                            for m = 1, #value, 2 do
                                data[#data + 1] = value[m]
                            end
                            value = data
                        elseif (agg_type == 'stats') then
                            value = to_hash(value)
                        end
                        temp[agg_type] = value
                    else
                        temp[#temp + 1] = agg_type
                        temp[#temp + 1] = value
                    end
                    if (bucket_hash[k] == nil) then
                        bucket_hash[k] = 1
                        bucket_list[#bucket_list + 1] = { tonumber(k), k }
                    end
                end
            end
        end

        -- use bucket_list to transform hash into properly ordered indexed array
        table.sort(bucket_list, function(a, b) return a[1] < b[1] end)

        local final = {}
        if (format == 'json') or (format == 'msgpack') then
            for i, ts in ipairs(bucket_list) do
                final[i] = { ts[1], result[ts[2]] }
            end
            final = format_result(final, format)
        else
            k = 1
            for _, ts in ipairs(bucket_list) do
                final[k] = ts[1]
                final[k + 1] = to_bulk_reply(result[ts[2]])
                k = k + 2
            end
        end
        return final
    end

    local params = parse_range_params(PARAMETER_OPTIONS, min, max, ...)
    local data = base_range(cmd, key, params)
    local format = params.format
    if data and #data > 0 then
        local range = process_range(data, params)
        if params.aggregate ~= nil then
            range = handle_aggregation(range, params.aggregate, format)
        else
            if (format == 'json') or (format == 'msgpack') then
                local final = {}
                for i, value in ipairs(range) do
                    final[i] = {value[1], to_hash(value[2])}
                end
                range = format_result(final, format)
            end
        end

        if remove then
            remove_values(key, data, params.filter)
        end
        return range
    end

    return {}
end

function Timeseries.range(key, min, max, ...)
    return Timeseries._range(false,'zrangebylex', key, min, max, ...)
end

function Timeseries.revrange(key, min, max, ...)
    return Timeseries._range(false,'zrevrangebylex', key, min, max, ...)
end

-- Remove and return a range between *min* and *max*
function Timeseries.poprange(key, min, max, ...)
    return Timeseries._range(true, 'zrangebylex', key, min, max, ...);
end


-- list of timestamps between *min* and *max*
function Timeseries.times(key, min, max)
    min = min or '-'
    max = max or '+'
    local range = Timeseries.range(key, min, max)
    local result = {}
    for i, v in ipairs(range) do
        result[i] = tonumber(v[1] or 0)
    end
    return result
end

local function storeHash(dest, range)
    local args = {}
    for _, val in ipairs(range) do
        local ts = val[1]
        local data = val[2]

        if type(data) == 'table' then
            data = cjson.encode( to_hash(data) )
        end
        args[#args + 1] = tostring(ts)
        args[#args + 1] = data
    end
    redis.call('hmset', dest, unpack(args))
end

local function storeTimeseries(dest, range)
    for _, val in ipairs(range) do
        local ts = val[1]
        local data = val[2]
        if type(data) ~= 'table' then
            data = {'value', data}
        end
        Timeseries.add(dest, ts, unpack(data))
    end
end

--- copy data from a timeseries and store it in another key
function Timeseries.copy(key, dest, min, max, ...)

    local function handle_aggregation(range, agg_params, storage)
        local aggregate = aggregate
        local by_key = {}
        for _, v in ipairs(range) do
            local ts = v[1]
            local hash = v[2] or {}
            for i = 1, #hash, 2 do
                local k = hash[i]
                by_key[k] = by_key[k] or {}
                table.insert(by_key[k], { ts, hash[i + 1] })
            end
        end
        local result = {}
        local bucket_hash = {}
        local bucket_list = {}
        local sep = '_'

        for _, field_info in ipairs(agg_params.fields) do
            local key = field_info[1]
            local agg_type = field_info[2]
            local values = by_key[ key ]

            if (values and #values > 0) then
                local buckets = aggregate(values, agg_type, agg_params.timeBucket)
                for k, value in pairs(buckets) do
                    result[k] = result[k] or {}

                    local slot_key = key .. sep .. agg_type
                    local temp = result[k]
                    if (type(value) == 'table') then
                        for j = 1, #value, 2 do
                            temp[#temp + 1] = slot_key .. sep .. value[j]
                            temp[#temp + 1] = value[j + 1]
                        end
                    else
                        temp[#temp + 1] = slot_key
                        temp[#temp + 1] = value
                    end
                    if (bucket_hash[k] == nil) then
                        bucket_hash[k] = 1
                        bucket_list[#bucket_list + 1] = { tonumber(k), k }
                    end
                end
            end
        end

        -- use bucket_list to transform hash into properly ordered indexed array
        table.sort(bucket_list, function(a, b) return a[1] < b[1] end)
        local final = {}
        for i, ts in ipairs(bucket_list) do
            final[i] = { ts[1], result[ts[2]] }
        end
        return final
    end

    local params = parse_range_params(COPY_OPTIONS, min, max, ...)
    local storage = params.storage or 'timeseries'
    local data = base_range('zrangebylex', key, params)

    if (#data == 0) then
        return 0
    end

    -- Fast path if no filtering or transformation
    if (params.filter == nil and params.aggregate == nil and storage == 'timeseries') and (params.labels == nil) and (params.redacted == nil) then
        for _, val in ipairs(data) do
            redis.call('zadd', dest, 0, val)
        end
        return #data
    end

    if (params.aggregate ~= nil) then
        -- if we're aggregating, the user should select a label
        if (#params.aggregate.fields == 0) then
            params.label = { value = 1 }
        end
    end

    local range = process_range(data, params)
    if params.aggregate ~= nil then
        range = handle_aggregation(range, params.aggregate, storage)
    end

    if (#range) then
        if storage == 'timeseries' then
            storeTimeseries(dest, range)
        else
            storeHash(dest, range)
        end
    end

    return #range
end

---------
local UpperMap

local command_name = assert(table.remove(ARGV, 1), 'Timeseries: must provide a command')
local command = Timeseries[command_name]
if (command == nil) then
    if UpperMap == nil then
        UpperMap = {}
        for name, func in pairs(Timeseries) do
            if name:sub(1, 1) ~= '_' then
                UpperMap[name:upper()] = func
            end
        end
    end
    command_name = string.upper(command_name)
    command = UpperMap[command_name]
end
if (command == nil) then
    error('Timeseries: unknown command ' .. command_name)
end

-- ts_debug('running ' .. command_name .. '(' .. KEYS[1] .. ',' .. table.tostring(ARGV) .. ')')

if (command_name == 'copy') or (command_name == 'COPY') then
    return command(KEYS[1], KEYS[2], unpack(ARGV))
end

local result = command(KEYS[1], unpack(ARGV))

return result