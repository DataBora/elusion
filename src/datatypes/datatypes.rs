use datafusion::arrow::datatypes::DataType as ArrowDataType;

#[derive(Debug, Clone)]
pub enum SQLDataType {
    // Character Types
    Char,
    Varchar,
    Text,
    String,

    // Numeric Types
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    TinyIntUnsigned,
    SmallIntUnsigned,
    IntUnsigned,
    BigIntUnsigned,
    Float,
    Real,
    Double,
    Decimal(u8, u8), // precision, scale

    // Date/Time Types
    Date,
    Time,
    Timestamp,
    Interval,

    // Boolean Types
    Boolean,

    // Binary Types
    ByteA,

    // Unsupported Types
    Unsupported(String),
}

impl From<SQLDataType> for ArrowDataType {
    fn from(sql_type: SQLDataType) -> Self {
        match sql_type {
            // Character Types
            SQLDataType::Char | SQLDataType::Varchar | SQLDataType::Text | SQLDataType::String => ArrowDataType::Utf8,

            // Numeric Types
            SQLDataType::TinyInt => ArrowDataType::Int8,
            SQLDataType::SmallInt => ArrowDataType::Int16,
            SQLDataType::Int => ArrowDataType::Int32,
            SQLDataType::BigInt => ArrowDataType::Int64,
            SQLDataType::TinyIntUnsigned => ArrowDataType::UInt8,
            SQLDataType::SmallIntUnsigned => ArrowDataType::UInt16,
            SQLDataType::IntUnsigned => ArrowDataType::UInt32,
            SQLDataType::BigIntUnsigned => ArrowDataType::UInt64,
            SQLDataType::Float | SQLDataType::Real => ArrowDataType::Float32,
            SQLDataType::Double => ArrowDataType::Float64,
            
            
            // SQLDataType::Decimal(precision, scale) => 
            // {
            //     let precision_u8 = precision.try_into().unwrap();
            //     let scale_i8 = scale.try_into().unwrap();
            //     ArrowDataType::Decimal128(precision_u8, scale_i8)
            // }
            SQLDataType::Decimal(precision, scale) => ArrowDataType::Decimal128(precision.into(), scale.try_into().unwrap()),

            // Date/Time Types
            SQLDataType::Date => ArrowDataType::Date32,
            SQLDataType::Time => ArrowDataType::Time64(datafusion::arrow::datatypes::TimeUnit::Nanosecond),
            SQLDataType::Timestamp => ArrowDataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            SQLDataType::Interval => ArrowDataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),

            // Boolean Types
            SQLDataType::Boolean => ArrowDataType::Boolean,

            // Binary Types
            SQLDataType::ByteA => ArrowDataType::Binary,

            // Unsupported
            SQLDataType::Unsupported(msg) => panic!("Unsupported SQL type: {}", msg),
        }
    }
}

impl SQLDataType {
    pub fn from_str(data_type: &str) -> Self {
        match data_type.to_uppercase().as_str() {
            "CHAR" => SQLDataType::Char,
            "VARCHAR" => SQLDataType::Varchar,
            "TEXT" | "STRING" => SQLDataType::Text,
            "TINYINT" => SQLDataType::TinyInt,
            "SMALLINT" => SQLDataType::SmallInt,
            "INT" | "INTEGER" => SQLDataType::Int,
            "BIGINT" => SQLDataType::BigInt,
            "FLOAT" => SQLDataType::Float,
            "DOUBLE" => SQLDataType::Double,
            "DECIMAL" => SQLDataType::Decimal(20, 4), 
            "NUMERIC" | "NUMBER" => SQLDataType::Decimal(20,4),
            "DATE" => SQLDataType::Date,
            "TIME" => SQLDataType::Time,
            "TIMESTAMP" => SQLDataType::Timestamp,
            "BOOLEAN" => SQLDataType::Boolean,
            "BYTEA" => SQLDataType::ByteA,
            _ => SQLDataType::Unsupported(data_type.to_string()),
        }
    }
}

impl From<ArrowDataType> for SQLDataType {
    fn from(arrow_type: ArrowDataType) -> Self {
        match arrow_type {
            ArrowDataType::Utf8 => SQLDataType::String,
            ArrowDataType::Int8 => SQLDataType::TinyInt,
            ArrowDataType::Int16 => SQLDataType::SmallInt,
            ArrowDataType::Int32 => SQLDataType::Int,
            ArrowDataType::Int64 => SQLDataType::BigInt,
            ArrowDataType::UInt8 => SQLDataType::TinyIntUnsigned,
            ArrowDataType::UInt16 => SQLDataType::SmallIntUnsigned,
            ArrowDataType::UInt32 => SQLDataType::IntUnsigned,
            ArrowDataType::UInt64 => SQLDataType::BigIntUnsigned,
            ArrowDataType::Float32 => SQLDataType::Float,
            ArrowDataType::Float64 => SQLDataType::Double,
            ArrowDataType::Date32 => SQLDataType::Date,
            ArrowDataType::Time64(_) => SQLDataType::Time,
            ArrowDataType::Timestamp(_, _) => SQLDataType::Timestamp,
            ArrowDataType::Boolean => SQLDataType::Boolean,
            ArrowDataType::Binary => SQLDataType::ByteA,
            _ => SQLDataType::Unsupported(format!("{:?}", arrow_type)),
        }
    }
}
