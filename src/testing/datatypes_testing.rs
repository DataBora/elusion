use crate::datatypes::datatypes::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_to_arrow_mapping() {
        assert_eq!(ArrowDataType::from(SQLDataType::Char), ArrowDataType::Utf8);
        assert_eq!(ArrowDataType::from(SQLDataType::Int), ArrowDataType::Int32);
        assert_eq!(ArrowDataType::from(SQLDataType::Date), ArrowDataType::Date32);
    }

    #[test]
    fn test_arrow_to_sql_mapping() {
        assert_eq!(SQLDataType::from(ArrowDataType::Utf8), SQLDataType::String);
        assert_eq!(SQLDataType::from(ArrowDataType::Int32), SQLDataType::Int);
        assert_eq!(SQLDataType::from(ArrowDataType::Boolean), SQLDataType::Boolean);
    }
}
