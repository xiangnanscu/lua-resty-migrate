local cjson_encode = require "cjson.safe".encode
local utils = require "resty.utils"
local object = require "resty.object"
local array = require "resty.array"
local format = string.format

local function serialize_defaut(val)
  if type(val) == "string" then
    return "'" .. val:gsub("'", "''") .. "'"
  elseif val == false then
    return "FALSE"
  elseif val == true then
    return "TRUE"
  elseif type(val) == "number" then
    return tostring(val)
  elseif type(val) == "function" then
    return serialize_defaut(val())
  elseif type(val) == "table" then
    local s, err = cjson_encode(val)
    if err then
      return nil, "table as a default value but can not be encoded"
    end
    return serialize_defaut(s)
  else
    return nil, format("type `%s` is not supported as a default value", type(val))
  end
end

-- 所有compare_XXX都应该在此登记
local compare_names = {
  'type',
  'db_type',
  'null',
  'default',
  'index',
  'unique',
  'primary_key',
  "serial",
  'maxlength',
  'length',
  'timezone',
  'precision',
  'reference_column',
  'reference',
  'on_delete',
  'on_update',
  'auto_now_add',
  'label'
}
local function serialize_field(field)
  local tokens = {}
  for _, name in ipairs(compare_names) do
    local token = field[name]
    if name == 'reference' and token then
      token = token.table_name
    end
    tokens[#tokens + 1] = format("%s:%s", name, token)
  end
  return table.concat(tokens, '|')
end

local function get_foreign_field(field)
  local fk_model = field.reference
  local fk_name = field.reference_column or
      assert(fk_model.primary_key,
        format("model '%s' referenced by foreignkey must define primary_key", fk_model.table_name))
  local fk = assert(fk_model.fields[fk_name],
    format("invalid field name '%s' for table '%s'", fk_name, fk_model.table_name))
  return fk
end

local function make_migrate_fields(ctx)
  local basefield = utils.class { table_name = ctx.table_name }
  function basefield.get_create_type(self)
    return assert(self.type_string)
  end

  function basefield.serialized(self)
    return serialize_field(self.field)
  end

  function basefield.compare(self, old_feild)
    local new_field = self.field
    local tokens = {}
    for _, name in ipairs(compare_names) do
      local method = self['compare_' .. name]
      if method then
        local token, whole = method(self, old_feild[name], new_field[name], old_feild, new_field)
        if token then
          tokens[#tokens + 1] = whole and token or format("ALTER TABLE %s %s", self.table_name, token)
        end
      end
    end
    return tokens
  end

  function basefield.compare_type(self, old, new, old_field)
    local new_field = self.field
    if old == new then
    elseif new == 'string' or new == 'integer' then
      local col = new_field.name
      local default_token = new_field.default ~= nil and format("SET DEFAULT %s", new_field.default) or ''
      return format("ALTER COLUMN %s TYPE %s USING(%s::%s)", col, self:get_create_type(), col, new)
    elseif new == 'text' and old == 'string' then
      local col = new_field.name
      return format("ALTER COLUMN %s TYPE %s", col, self:get_create_type())
    elseif new == 'year_month' then
      local col = new_field.name
      return format("ALTER COLUMN %s TYPE %s USING(%s::%s)", col, self:get_create_type(), col, new_field.db_type)
    elseif new_field.db_type == old_field.db_type then
    else
      error(format("table `%s` field `%s` alter type from `%s` to `%s` is not supported", ctx.table_name, new_field.name,
        old, new))
    end
  end

  function basefield.compare_null(self, old, new)
    old = not not old
    new = not not new
    if old == true and new == false then
      return format("ALTER COLUMN %s SET NOT NULL", self.field.name)
    elseif old == false and new == true then
      return format("ALTER COLUMN %s DROP NOT NULL", self.field.name)
    else
    end
  end

  function basefield.compare_default(self, old, new)
    if old == new then
    elseif type(old) == 'table' and type(new) == 'table' and object.equals(old, new) then
    elseif new ~= nil then
      return format("ALTER COLUMN %s SET DEFAULT %s", self.field.name, serialize_defaut(new))
    elseif new == nil then
      return format("ALTER COLUMN %s DROP DEFAULT", self.field.name)
    else
    end
  end

  function basefield.compare_auto_now_add(self, old, new)
    if old == new then
    elseif new then
      return format("ALTER COLUMN %s SET DEFAULT CURRENT_TIMESTAMP", self.field.name)
    else
      return format("ALTER COLUMN %s DROP DEFAULT", self.field.name)
    end
  end

  function basefield.compare_primary_key(self, old, new)
    old = not not old
    new = not not new
    if old == true and new == false then
      return format("DROP CONSTRAINT %s_pkey", self.field.name)
    elseif old == false and new == true then
      return format("ADD PRIMARY KEY (%s)", self.field.name)
    else
    end
  end

  function basefield.compare_unique(self, old, new)
    old = not not old
    new = not not new
    if old == true and new == false then
      return format("DROP CONSTRAINT %s", self:get_unique_constraint_name())
    elseif old == false and new == true then
      return format("ADD CONSTRAINT %s UNIQUE (%s)", self:get_unique_constraint_name(), self.field.name)
    else
    end
  end

  function basefield.get_unique_constraint_name(self)
    return format("%s_%s_key", self.table_name, self.field.name)
  end

  function basefield.get_index_constraint_name(self)
    return format("%s_%s_idx", self.table_name, self.field.name)
  end

  function basefield.get_foreign_key_constraint_name(self)
    return format("%s_%s_fkey", self.table_name, self.field.name)
  end

  function basefield.get_create_index_token(self)
    return format("CREATE INDEX %s ON %s (%s)", self:get_index_constraint_name(), self.table_name, self.field.name)
  end

  function basefield.compare_index(self, old, new)
    old = not not old
    new = not not new
    if old == true and new == false then
      return format("DROP INDEX %s", self:get_index_constraint_name()), true
    elseif old == false and new == true then
      return self:get_create_index_token(), true
    else
    end
  end

  function basefield.get_create_tokens(self)
    local tokens = {}
    local table_tokens = {}
    local field = self.field
    if field.label then
      -- table_tokens[#table_tokens + 1] = format("COMMENT ON COLUMN %s.%s IS '%s'", self.table_name, field.name,
      --   field.label)
    end
    if field.primary_key then
      field.null = false
      tokens[#tokens + 1] = 'PRIMARY KEY'
    end
    if field.serial then
      field.null = false
      function self.get_create_type()
        return "SERIAL"
      end

      -- tokens[#tokens + 1] = 'SERIAL'
    end
    if field.null then
      -- tokens[#tokens + 1] = 'NULL'
    else
      tokens[#tokens + 1] = 'NOT NULL'
    end
    -- handle default
    if field.type == "datetime" then
      if field.auto_now_add then
        tokens[#tokens + 1] = 'DEFAULT CURRENT_TIMESTAMP'
      end
    elseif field.default ~= nil then
      local val, err = serialize_defaut(field.default)
      if err then
        error(format("error when processing default value of field %s of %s: %s", field.name, self.table_name,
          err))
      end
      tokens[#tokens + 1] = "DEFAULT " .. val
    end
    if field.unique then
      tokens[#tokens + 1] = "UNIQUE"
    elseif field.index then
      table.insert(table_tokens, self:get_create_index_token())
    end
    return table.concat(tokens, " "), table_tokens
  end

  -- function basefield.foo(self)

  -- end
  local string = utils.class({}, basefield)
  function string.get_create_type(self)
    return format("varchar(%s)", self.field.maxlength or self.field.length)
  end

  function string.compare_maxlength(self, old, new)
    if old ~= new then
      return format("ALTER COLUMN %s TYPE varchar(%s)", self.field.name, new)
    else
    end
  end

  function string.compare_length(self, old, new)
    if old ~= new then
      return format("ALTER COLUMN %s TYPE varchar(%s)", self.field.name, new)
    else
    end
  end

  local text = utils.class({ type_string = "text" }, basefield)
  function text.get_create_type(self)
    return "text"
  end

  local sfzh = utils.class({}, string)
  local password = utils.class({}, string)
  local email = utils.class({}, string)
  local alioss = utils.class({}, string)
  local alioss_image = utils.class({}, string)
  local integer = utils.class({ type_string = "integer" }, basefield)
  local float = utils.class({}, basefield)
  function float.get_create_type(self)
    if not self.field.precision then
      return format("float")
    else
      -- https://www.postgresql.org/docs/9.2/static/datatype-numeric.html#DATATYPE-FLOAT
      return format("float(%s)", self.field.precision)
    end
  end

  local json = utils.class({ type_string = "jsonb" }, basefield)
  local array = utils.class({}, json)
  local table = utils.class({}, array)
  local alioss_list = utils.class({ type_string = "jsonb" }, array)
  local alioss_image_list = utils.class({ type_string = "jsonb" }, array)
  local boolean = utils.class({ type_string = "boolean" }, basefield)
  local date = utils.class({ type_string = 'date' }, basefield)
  local year = utils.class({ type_string = 'integer' }, string)
  function year.get_create_type(self)
    return "integer"
  end

  local month = utils.class({ type_string = 'integer' }, string)
  function month.get_create_type(self)
    return "integer"
  end

  local year_month = utils.class({ type_string = 'varchar' }, string)
  function year_month.get_create_type(self)
    return "varchar"
  end

  local datetime = utils.class({ type_string = 'timestamp' }, basefield)
  function datetime.get_create_type(self)
    local timezone_token = self.field.timezone and " WITH TIME ZONE" or ""
    return format("%s(%s)%s", self.type_string, self.field.precision or 0, timezone_token)
  end

  function datetime.compare_precision(self, old, new)
    if old ~= new then
      local timezone_token = self.field.timezone and " WITH TIME ZONE" or ""
      return format("ALTER COLUMN %s TYPE %s(%s)%s", self.field.name, self.type_string, new, timezone_token)
    else
    end
  end

  function datetime.compare_timezone(self, old, new)
    if old ~= new then
      local timezone_token = new and " WITH TIME ZONE" or ""
      return format("ALTER COLUMN %s TYPE %s(%s)%s", self.field.name, self.type_string, self.field.precision,
        timezone_token)
    else
    end
  end

  local time = utils.class({ type_string = 'time' }, datetime)
  local foreignkey = utils.class({}, basefield)
  local function same_reference(old, new)
    for _, attr in ipairs({ 'column', 'primary_key', 'table_name', 'on_delete', 'on_update' }) do
      if old[attr] ~= new[attr] then
        return false, format("%s old(%s) new(%s)", attr, old[attr], new[attr])
      end
    end
    -- 对于被引用的foreign field,仅比较name和type. 因为maxlength等之类的变化并不改变引用关系
    for name, field in pairs(old.fields) do
      local new_field = new.fields[name]
      if not (field.db_type == new_field.db_type and field.name == new_field.name) then
        return false,
            format("%s old(%s,%s) new(%s,%s)", name, field.db_type, field.name, new_field.db_type, new_field.name)
      end
    end
    return true
  end

  function foreignkey.compare_reference(self, old, new)
    local same, hint = same_reference(old, new)
    if not same then
      print(ctx.table_name, ':', self.field.name, ':', hint)
    end
    if same then
      -- same
    elseif old == nil then
      -- add
    elseif new ~= nil then
      -- alter
      return format("%s, %s", self:drop_fk_constraint(), self:add_fk_constraint())
    else
      -- delete
    end
  end

  function foreignkey.drop_fk_constraint(self)
    local conname = self:get_foreign_key_constraint_name()
    return format("DROP CONSTRAINT %s", conname)
  end

  function foreignkey.add_fk_constraint(self)
    local conname = self:get_foreign_key_constraint_name()
    local ref_token = self:get_ref_token()
    return format("ADD CONSTRAINT %s FOREIGN KEY (%s) %s", conname, self.field.name, ref_token)
  end

  function foreignkey.get_ref_token(self)
    local field = self.field
    local fk_model = field.reference
    local fk = get_foreign_field(field)
    local fk_name = fk.name
    local ref_token = format('REFERENCES "%s" ("%s") ON DELETE %s ON UPDATE %s', fk_model.table_name, fk_name,
      field.on_delete or "NO ACTION", field.on_update or "CASCADE")
    return ref_token
  end

  function foreignkey.get_create_type(self)
    local field = self.field
    local ref_token = self:get_ref_token()
    local mfields = make_migrate_fields { table_name = self.table_name }
    local ref_type
    while 1 do
      local fk = get_foreign_field(field)
      local fk_cls = assert(mfields[fk.type], "invalid foreignkey type:" .. fk.type)
      if fk.type ~= "foreignkey" then
        ref_type = fk_cls:new { field = fk }:get_create_type()
        break
      else
        field = fk
      end
    end
    return format("%s %s", ref_type, ref_token)
  end

  return {
    string = string,
    text = text,
    integer = integer,
    float = float,
    datetime = datetime,
    date = date,
    time = time,
    json = json,
    array = array,
    table = table,
    foreignkey = foreignkey,
    boolean = boolean,
    sfzh = sfzh,
    password = password,
    email = email,
    year_month = year_month,
    year = year,
    month = month,
    alioss = alioss,
    alioss_list = alioss_list,
    alioss_image_list = alioss_image_list,
    alioss_image = alioss_image
  }
end

local JSON_TYPES = {
  string = true,
  number = true,
  boolean = true,
  table = true,
}
local function make_field_for_compare(field)
  local res = {}
  for _, name in ipairs(compare_names) do
    local attr = field[name]
    if JSON_TYPES[type(attr)] then
      res[name] = attr
    end
  end
  res.name = field.name
  local fk_model = field.reference
  if fk_model then
    res.table_name = field.table_name
    -- 为避免栈溢出, 按需复制reference属性
    local fk = get_foreign_field(field)
    local mfk = make_field_for_compare(fk)
    -- if not fk_model.table_name then
    --   utils.loger(fk_model)
    -- end
    res.reference = {
      table_name = assert(fk_model.table_name),
      -- field_names = fk_model.field_names,
      column = field.reference_column,
      fields = { [fk.name] = mfk },
      -- field = mfk, -- 为了方便same_reference
      primary_key = fk_model.primary_key,
      -- 添加这两个属性为了方便compare_reference,不用再单独写compare_on_delete
      on_delete = field.on_delete,
      on_update = field.on_update
    }
  end
  return res
end

local function make_model_for_compare(model)
  local names = utils.list(model.field_names)
  local fields = {}
  for i, name in ipairs(names) do
    local field = model.fields[name]
    fields[name] = make_field_for_compare(field)
  end
  local cmodel = {
    field_names = names,
    fields = fields,
    table_name = model.table_name,
    unique_together = model.unique_together
  }
  return cmodel
end

local function get_field(mfields, field)
  local mfield = assert(mfields[field.type], format("invalid field type:%s", field.type))
  return mfield:new { field = field }
end
local function to_sorted_key(arr)
  return array(arr):slice():sort():join(',')
end
-- format("ALTER TABLE %s ADD CONSTRAINT %s_%s_key UNIQUE (%s)", model.table_name,
-- model.table_name, unique_group:join('_'), unique_group:join(', '))
local function compare_unique_together(old, new)
  local tokens = array {}
  local old_map = {}
  local new_map = {}
  for i, uniques in ipairs(old.unique_together or {}) do
    old_map[to_sorted_key(uniques)] = array(uniques)
  end
  for i, uniques in ipairs(new.unique_together or {}) do
    new_map[to_sorted_key(uniques)] = array(uniques)
  end
  for key, uniques in pairs(old_map) do
    if not new_map[key] then
      tokens:push(format("ALTER TABLE %s DROP CONSTRAINT %s_%s_key", new.table_name,
        new.table_name, uniques:join('_'), uniques:join(', ')))
    end
  end
  for key, uniques in pairs(new_map) do
    if not old_map[key] then
      tokens:push(format("ALTER TABLE %s ADD CONSTRAINT %s_%s_key UNIQUE (%s)", new.table_name,
        new.table_name, uniques:join('_'), uniques:join(', ')))
    end
  end
  return tokens
end
local function compare_models(old, new)
  if type(new.field_names) ~= 'table' then
    print(format("invalid model json:%s,%s", new.table_name, type(new.field_names)))
    return {}
  end
  local mfields = make_migrate_fields { table_name = new.table_name }
  local all_tokens = compare_unique_together(old, new)
  local rename_cols = {}
  local function try_find_rename_column(new_field)
    local new_serialized = get_field(mfields, new_field):serialized()
    local columns = {}
    for name, old_field in pairs(old.fields) do
      local old_serialized = get_field(mfields, old_field):serialized()
      if new_serialized == old_serialized then
        columns[#columns + 1] = name
      end
    end
    if #columns == 1 then
      return columns[1]
    else
      return
    end
  end

  for i, name in ipairs(new.field_names) do
    local new_field = new.fields[name]
    local old_field = old.fields[name]
    if not new_field then
      error(format("invalid field name %s for model %s", name, new.table_name))
    elseif not old_field then
      -- add column or rename column
      local old_name = try_find_rename_column(new_field)
      if old_name then
        rename_cols[old_name] = true
        all_tokens[#all_tokens + 1] = format("ALTER TABLE %s RENAME %s TO %s", new.table_name, old_name, name)
      else
        local mf = get_field(mfields, new_field)
        local create_token, table_tokens = mf:get_create_tokens()
        local add_token = format("ALTER TABLE %s ADD COLUMN %s %s %s", new.table_name, name, mf:get_create_type(),
          create_token)
        all_tokens[#all_tokens + 1] = add_token
        utils.list_extend(all_tokens, table_tokens)
      end
      -- same name
    elseif old_field.type == new_field.type then
      local mfield = mfields[new_field.type]
      if mfield then
        local tokens = mfield:new { field = new_field }:compare(old_field)
        if #tokens > 0 then
          utils.list_extend(all_tokens, tokens)
        end
      end
    else
      -- TODO: alter column type, such as add/drop foreign key constraint, varchar vs text vs integer
      if old_field.reference and not new_field.reference then
        local mfield = mfields[old_field.type]
        all_tokens[#all_tokens + 1] = format("ALTER TABLE %s %s", new.table_name,
          mfield:new { field = old_field }:drop_fk_constraint())
      elseif new_field.reference and not old_field.reference then
        local mfield = mfields[new_field.type]
        all_tokens[#all_tokens + 1] = format("ALTER TABLE %s %s", new.table_name,
          mfield:new { field = new_field }:add_fk_constraint())
      else
        local mfield = mfields[new_field.type]
        if mfield then
          local tokens = mfield:new { field = new_field }:compare(old_field)
          if #tokens > 0 then
            utils.list_extend(all_tokens, tokens)
          end
        end
      end
    end
  end
  for i, name in ipairs(old.field_names) do
    local new_field = new.fields[name]
    -- local old_field = old.fields[name]
    if not new_field and not rename_cols[name] then
      all_tokens[#all_tokens + 1] = format("ALTER TABLE %s DROP COLUMN %s", new.table_name, name)
    end
  end
  return all_tokens
end

local function create_model(model_json)
  local column_tokens = {}
  local table_tokens = {}
  local mfields = make_migrate_fields { table_name = model_json.table_name }
  for _, name in ipairs(model_json.field_names) do
    local field = model_json.fields[name]
    local mf = get_field(mfields, field)
    local create_token, _table_tokens = mf:get_create_tokens()
    local add_token = format("%s %s %s", name, mf:get_create_type(), create_token)
    column_tokens[#column_tokens + 1] = add_token
    utils.list_extend(table_tokens, _table_tokens)
  end
  for i, unique_group in ipairs(model_json.unique_together or {}) do
    column_tokens[#column_tokens + 1] = format("UNIQUE(%s)", table.concat(unique_group, ', '))
  end
  return column_tokens, table_tokens
end

local function get_table_defination(model_json)
  local column_tokens, table_tokens = create_model(model_json)
  local tmp_token = model_json.tmp and 'TEMPORARY ' or ''
  local column_token = table.concat(column_tokens, ",\n  ")
  local main_token = format("CREATE %sTABLE %s(\n  %s\n)", tmp_token, model_json.table_name, column_token)
  return table.concat(utils.list({ main_token }, table_tokens), ";\n")
end

return {
  make_migrate_fields = make_migrate_fields,
  make_model_for_compare = make_model_for_compare,
  compare_models = compare_models,
  get_table_defination = get_table_defination
}
