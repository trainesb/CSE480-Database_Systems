import json

class NotValidError(Exception):
  def __init__(self, obj, method):
    print(obj, " doesn't fit the schema")

class NotWellFormedError(Exception):
  def __init__(self, obj, method):
    print(obj, " isn't a JSON value")

class Schema:
  """Representation of a schema"""

  schema_type = None
  schema_qualities = None
  schema_element = None
  schema_fields = None
  field_schema = None
  field_type = None

  # Type may be object|null|boolean|array|number|integer|string
  def __init__(self, schema_string):
    self.create_schema(schema_string)

  def create_schema(self, schema_string):
    """Creates the initial schema when it's built

    :param schema_string: string representing the schema
    """
    schema_string = schema_string.replace(' ','').split(',', 1)

    if (len(schema_string) > 0) and (schema_string[0] != '{}'):
      self.schema_type = schema_string[0].split(':')[1]

      if '}' in self.schema_type:
        self.schema_type = self.schema_type.replace('}', '')

      if len(schema_string) > 1:
        if 'fields_qualities' in schema_string[1]:
          self.schema_fields = schema_string[1].split(':', 1)[1]

          if ',' in self.schema_fields:
            self.schema_fields = self.schema_fields.split(',')
            self.field_type = list()
            self.field_schema = list()

            for field in self.schema_fields:
              if '{' in field:
                field = field.replace('{', '')
              if '}' in field:
                field = field.replace('}', '')

              field = field.split(':', 1)
              self.field_schema.append(field)
              self.field_type.append(field[0])

          else:
            self.schema_fields = self.schema_fields.replace('{', '').replace('}', '').split(':', 1)
            self.field_type = self.schema_fields[0]
            self.field_schema = self.schema_fields

        elif 'fields' in schema_string[1]:
          self.schema_fields = schema_string[1].split(':', 1)[1]

          if '}' in self.schema_fields:
            self.schema_fields = self.schema_fields.replace('}', '')

        elif 'qualities' in schema_string[1]:
          self.schema_qualities = schema_string[1].split(":",1)[1]

          if 'element' in schema_string[1]:
            self.schema_element = schema_string[1].split(',', 1)[1].split(":",1)[1]
            self.schema_qualities = schema_string[1].split(',',1)[0]

  def validate(self, value):
    """When validate is called, the proper function is called

    :param value: the value to validate
    """
    if self.schema_type == '"integer"':
      return self.validate_int(value)
    elif self.schema_type == '"array"':
      return self.validate_array(value)
    elif self.schema_type == '"object"':
      return self.validate_obj(value)
    elif self.schema_type == '"string"':
      return self.validate_str(value)
    else:
      return self.validate_empty(value)

  def validate_int(self, value):
    """Validates when type is integer"""

    value = value.replace('{', '').replace('}', '')
    if value == "null":
      raise NotValidError(value, self.validate_int)
    if '.' in value:
      if value.replace('.', '').isdigit():
        raise NotValidError(value, self.validate_int)
    elif not value.isdigit():
      raise NotValidError(value, self.validate_int)

  def validate_array(self, value):
    """Validates when the type is an array"""

    if '"ascending"' in self.schema_qualities:
      self.ascending_array(value)
    if '"unique"' in self.schema_qualities:
      self.unique_array(value)
    if '"nonempty"' in self.schema_qualities:
      self.nonempty_array(value)
    if '"spartan"' in self.schema_qualities:
      self.spartan_array(value)
    if self.schema_element is not None:
      self.validate_element(value)

  def ascending_array(self, values):
    """Validates when type is array and qualities is ascending"""

    if '],' not in values:
      values = self.validate_values(values)
    else:
      values = values.split('],')

      for i in range(len(values)):
        if '[' in values[i]:
          values[i] = values[i].replace('[', '')
        if ']' in values[i]:
          values[i] = values[i].replace(']', '')
        if ' ' in values[i]:
          values[i] = values[i].replace(' ', '')
        if ',' in values[i]:
          values[i] = values[i].split(',')

        self.validate(values[i])

    if len(values) > 1:
      prev = values[0]

      for val in values:
        if not val.isdigit():
          if prev > val:
            raise NotValidError(values, self.validate_array)

        else:
          if int(val) < int(prev):
            raise NotValidError(values, self.validate_array)

        prev = val

  def unique_array(self, values):
    """When type is array and qualities is unique"""

    values = self.validate_values(values)
    if len(values) > len(set(values)):
      raise NotValidError(values, self.validate_array)

  def nonempty_array(self, values):
    """When type is array and qualities are nonempty"""

    values = self.validate_values(values)
    if len(values) == 0:
      raise NotValidError(values, self.validate_array)

  def spartan_array(self, values):
    """When type is array and qualities are spartan"""

    values = self.validate_values(values)
    if type(values) is str:
      if (values != '"green"') and (values != '"white"') and (len(values) > 0):
        raise NotValidError(values, self.spartan_array)
    else:
      for val in values:
        if (val != '"green"') and (val != '"white"') and (len(val) > 0):
          raise NotValidError(values, self.spartan_array)

  def validate_str(self, value):
    """When type is string"""

    value = value.replace('}','').replace('{','')
    if '"' not in value:
      raise NotValidError(value, self.validate_str)

  def validate_empty(self, value):
    """when type is empty"""

    value = value.replace('}', '').replace('{', '')

    if (value != "null") and ('"' not in value) and (not value.isdigit()):
      if '.' in value:
        if not value.replace('.', '').isdigit():
          raise NotWellFormedError(value, self.validate_empty)
      else:
        raise NotWellFormedError(value, self.validate_empty)
    if ('.' in value) and (len(value) < 3):
      raise NotWellFormedError(value, self.validate_empty)

  def validate_element(self, value):
    """When the schema has an element, this function is called to validate"""

    if '{' in self.schema_element:
      self.schema_element = self.schema_element.replace('{', '')
    if '}' in self.schema_element:
      self.schema_element = self.schema_element.replace('}', '')

    element_type = self.schema_element.split(":")[1]
    if '}' in element_type:
      element_type = element_type.replace('}', '')

    if element_type == '"boolean"':
      value = self.validate_values(value)

      if type(value) is str:
        if (value != 'true') and (value != 'false'):
          raise NotValidError(value, self.validate_element)

      else:
        for val in value:
          if (val != 'true') and (val != 'false'):
            raise NotValidError(value, self.validate_element)

    if element_type == '"string"':
      value = self.validate_values(value)

      for val in value:
        if ('"' not in val) and (val != '') and ("'" not in val):
          raise NotValidError(value, self.validate_element)

    if '"array"' in element_type:
      schema = build_validator('{' + self.schema_element + '}')
      schema.validate(value)

  def validate_obj(self, value):
    """When type of the schema is an object"""

    if self.field_schema is not None:
      self.validate_field_qual(value)
    else:
      self.validate_field(value)

  def validate_field_qual(self, value):
    """When the schema has a field quaitie"""

    value = value.replace('{', '').replace('}', '')
    value_field = list()

    if value == '{}':
      raise NotValidError(value, self.validate_field_qual)

    if ' ' in value:
      value = value.replace(' ', '')

    if ',' in value:
      value = value.split(',')
      for i in range(len(value)):
        value[i] = value[i].split(':')
        value_field.append(value[i][0])
    else:
      value = value.split(':')
      value_field = value


    if type(self.field_type) is list:
      for field in self.field_type:
        if field not in value_field:
          raise NotValidError(self.schema_fields, self.validate_field_qual)

      for schema in self.field_schema:
        bld = build_validator('{'+schema[1]+'}')
        for data in value:
          if schema[0] == data[0]:
            bld.validate('{'+data[1]+'}')
    else:
      if self.field_type != value_field[0]:
        raise NotValidError(self.schema_fields, self.validate_field_qual)

      bld = build_validator('{'+self.schema_fields[1]+'}')
      bld.validate('{'+value[1]+'}')

  def validate_field(self, value):
    """When they type is an object and has a field"""

    value = value.replace('{', '').replace('}', '')

    if len(value) == 0:
      raise NotValidError(value, self.validate_obj)

    value_field = value[0]
    self.schema_fields = self.validate_values(self.schema_fields)

    if '}' in self.schema_fields:
      self.schema_fields = self.schema_fields.replace('}', '')

    if type(self.schema_fields) is list:
      for field in self.schema_fields:
        field = field + ':'
        if field not in value:
          raise NotValidError(field, self.validate_obj)

      if ',' in value:
        if ' ' in value:
          value = value.replace(' ', '')
        value = value.split(',')

      for val in value:
        val = val.split(':')
        if len(val) != 2:
          raise NotWellFormedError(value, self.validate_obj)
    else:
      self.schema_fields = self.schema_fields + ':'

      if self.schema_fields not in value:
        raise NotValidError(self.schema_fields, self.validate_obj)

  def validate_values(self, value):
    """used to clean up value"""

    if '[' in value:
      value = value.replace('[', '', 1)
    if ']' in value:
      value = value.replace(']', '')
    if value == '[':
      return value + ']'
    if ' ' in value:
      value = value.replace(' ', '')
    if ',' in value:
      value = value.split(',')
    return value

def build_validator(schema_string):
  """Used to build the schema class"""

  return Schema(schema_string)
