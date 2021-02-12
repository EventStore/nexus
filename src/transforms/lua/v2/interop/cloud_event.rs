use super::util::table_to_timestamp;
use crate::event::cloud_event::{AttributeValue, Attributes, CloudEvent};
use rlua::prelude::*;

impl<'a> ToLua<'a> for CloudEvent {
    fn to_lua(self, ctx: LuaContext<'a>) -> LuaResult<LuaValue> {
        ctx.create_table_from(self.iter().map(|(k, v)| (k, v)))
            .map(LuaValue::Table)
    }
}

impl<'lua, 'a> ToLua<'lua> for AttributeValue<'a> {
    fn to_lua(self, lua: LuaContext<'lua>) -> LuaResult<LuaValue> {
        let value = match self {
            AttributeValue::SpecVersion(version) => {
                rlua::Value::String(lua.create_string(version.as_str())?)
            }
            AttributeValue::String(s) => rlua::Value::String(lua.create_string(s)?),
            AttributeValue::URI(u) => rlua::Value::String(lua.create_string(u.as_str())?),
            AttributeValue::URIRef(u) => rlua::Value::String(lua.create_string(u.as_str())?),
            AttributeValue::Boolean(b) => rlua::Value::Boolean(*b),
            AttributeValue::Integer(i) => rlua::Value::Integer(*i),
            AttributeValue::Time(t) => {
                rlua::Value::String(lua.create_string(t.to_rfc3339().as_str())?)
            }
        };

        Ok(value)
    }
}

impl<'a> FromLua<'a> for Attributes {
    fn from_lua(value: LuaValue<'a>, _: LuaContext<'a>) -> LuaResult<Self> {
        let table = match &value {
            LuaValue::Table(table) => table,
            other => {
                return Err(LuaError::FromLuaConversionError {
                    from: other.type_name(),
                    to: "CloudEvent",
                    message: Some("Cloud event should be a Lua table".to_string()),
                })
            }
        };

        let id = table.get("id")?;
        let ty = table.get("type")?;
        let source = table.get("source")?;
        let data_content_type: Option<String> = table.get("data_content_type")?;
        let data_schema = table.get::<_, Option<String>>("data_schema")?;
        let subject: Option<String> = table.get("subject")?;
        let time = table
            .get::<_, Option<LuaTable>>("time")?
            .map(table_to_timestamp)
            .transpose()?;

        Ok(Attributes {
            id,
            ty,
            source,
            data_content_type,
            data_schema,
            subject,
            time,
        })
    }
}

impl<'a> FromLua<'a> for CloudEvent {
    fn from_lua(value: LuaValue<'a>, ctx: LuaContext<'a>) -> LuaResult<Self> {
        let attributes = Attributes::from_lua(value, ctx)?;

        Ok(CloudEvent {
            attributes,
            data: None,
            extensions: Default::default(),
        })
    }
}
