-- create table in postgresql database from model
local cjson_encode = require "cjson.safe".encode

local version = '1.0'

local function dict(a, b)
    local t = {}
    if a then
        for k, v in pairs(a) do
            t[k] = v
        end
    end
    if b then
        for k, v in pairs(b) do
            t[k] = v
        end
    end            
    return t
end


local function make_file(fn, content)
  local f, e = io.open(fn, "w+")
  if not f then
    return nil, e
  end
  local res, err = f:write(content)
  if not res then
    return nil, err
  end
  local res, err = f:close()
  if not res then
    return nil, err
  end
  return true
end
local function serialize_defaut(val)
    if type(val)=='string' then
        return "'"..val:gsub("'","''").."'" 
    elseif val == false then
        return 0
    elseif val == true then
        return 1
    elseif type(val) =='number' then
        return tostring(val)
    elseif type(val) == 'function' then
        return serialize_defaut(val())
    elseif type(val) == 'table' then
        local s, err = cjson_encode(val)
        if err then
            return nil, 'table as a default value but can not be encoded'
        end
        return serialize_defaut(s)
    else
        return nil, string.format('type `%s` is not supported as a default value', type(val))
    end
end

local function get_table_defination(model)
    local escn = model.sql.escape_name
    local joiner = ',\n    '
    local field_options = {indexes={}}
    local fields = {}
    local pk_defined = false
    local table_name = escn(model.table_name)

    -- table_options[#table_options+1] = 'DEFAULT CHARSET=utf8'
    for i, field in ipairs(model.fields) do
        local name = escn(field.name)
        local field_string
        local db_type = field.db_type:upper()
        local field_type = field.type

        if field.reference then
            local ref = string.format(
                'REFERENCES %s (id) ON DELETE %s ON UPDATE %s', 
                escn(field.reference.table_name), field.on_delete or 'CASCADE', field.on_update or 'CASCADE')
            -- **todo allow null
            table.insert(fields, string.format('%s INT NOT NULL %s', name, ref)) 
        elseif field.primary_key then
            assert(not pk_defined, 'you could set only one primary key')
            assert(field.name=='id', 'primary key name must be `id`')
            table.insert(fields, 'id SERIAL PRIMARY KEY')
            pk_defined = true
        else
            if db_type =='VARCHAR' then
                assert(field.maxlength, 'you must define maxlength')
                db_type = string.format('VARCHAR(%s)', field.maxlength)
                if field.default == nil then -- ** varchar always has a default value
                    db_type = db_type.." DEFAULT ''" 
                end
            end
            if field.index then
                table.insert(field_options.indexes, string.format('CREATE INDEX ON %s(%s)', table_name, name))
            end                
            if field.default ~= nil then
                -- 自动日期字段的默认值是函数, 调用之后会返回当前时间, 不能处理.
                if not (field.type == 'datetime' and (field.auto_now or field.auto_now_add)) then
                    local val, err = serialize_defaut(field.default)
                    if err then
                        return nil, string.format(
                            'error when processing default value of field %s of %s: %s', name, table_name, err)
                    end
                    db_type = db_type..' DEFAULT '..val
                end
            end
            if field.unique then
                db_type = db_type..' UNIQUE'
            end       
            if field.null then
                db_type = db_type..' NULL'
            else
                db_type = db_type..' NOT NULL'
            end    
            table.insert(fields, string.format('%s %s', name, db_type))
        end
    end

    local fields = table.concat(fields, joiner)
    local fo = {}
    for k, v in pairs(field_options) do -- flatten field_options
        if type(v) == 'table' then
            for i, e in ipairs(v) do
                fo[#fo+1] = e
            end
        else
            fo[#fo+1] = v
        end
    end
    local fields_create = string.format('CREATE TABLE %s(\n    %s)',table_name, fields)
    table.insert(fo, 1, fields_create)
    
    return table.concat(fo, ';\n    ')
end

local function drop_table(kwargs)
    -- first drop tables who have a foreignkey ralation to it.
    -- it's ok to attempt to drop a table repeatly for a simpler logic.
    for table_name, referenced_model in pairs(kwargs.model._referenced_models) do
        local res, err = drop_table({model=referenced_model, query=kwargs.query})
        if err then
            return nil, err
        end
    end
    local escape_name = kwargs.model.sql.escape_name(kwargs.model.table_name)
    local res, err = kwargs.query('DROP TABLE IF EXISTS '..escape_name)
    if not res then
        return nil, err
    end
    return true
end

local function save_model_to_db(kwargs)
    local table_defination, err = get_table_defination(kwargs.model)
    if err then
        return nil, err
    end
    local res, err = kwargs.query(string.format("SELECT relname FROM pg_class WHERE relname = '%s';", kwargs.model.table_name))
    if not res then
        return nil, err
    end
    if #res ~= 0 then
        if kwargs.drop_existed_table then
            local res, err = drop_table(kwargs)
            if not res then
                return nil, err
            end
        else
            return table_defination
        end
    end
    local res, err = kwargs.query(table_defination)
    if not res then
        return nil, err
    end
    return table_defination
end

local function save_models_to_db(kwargs)
    assert(kwargs.query and kwargs.models, 'you must provide models and query')
    local already = {}
    local function recursive(models, drop_existed_table, already)  
        for i, model in pairs(models) do
            local table_name = model.table_name
            if not already[table_name] then
                local fks = model.foreign_keys
                if next(fks) then -- 被引用的表需要先创建
                    local t = {}
                    for k, fk in pairs(fks) do
                        table.insert(t, fk.reference)
                    end
                    local res, err = recursive(t, drop_existed_table, already) 
                    if err then
                        return nil, err
                    end
                end
                local table_defination, err = save_model_to_db(dict(kwargs, {model=model}))
                if err then
                    return nil, err
                end
                already[table_name] = table_defination
            end
        end
        return already
    end
    return recursive(kwargs.models, kwargs.drop_existed_table, already) 
end


return {
    get_table_defination = get_table_defination,
    save_model_to_db = save_model_to_db,
    save_models_to_db = save_models_to_db,
}
    