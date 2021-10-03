mod binary;
mod boolean;
mod dictionary;
mod equal;
mod fixed_size_binary;
mod fixed_size_list;
mod growable;
mod list;
mod ord;
mod primitive;
mod union;
mod utf8;

use arrow2::array::{clone, new_empty_array, new_null_array, Array, PrimitiveArray};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field, IntervalUnit, PhysicalType};

fn all_datatypes() -> Vec<DataType> {
    use DataType::*;
    vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal(1, 1),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(3),
        DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
        DataType::LargeList(Box::new(Field::new("a", DataType::Binary, true))),
        DataType::FixedSizeList(Box::new(Field::new("a", DataType::Binary, true)), 4),
        DataType::Struct(vec![Field::new("a", DataType::Binary, true)]),
        DataType::Union(vec![Field::new("a", DataType::Binary, true)], None, true),
        DataType::Union(vec![Field::new("a", DataType::Binary, true)], None, false),
        DataType::Extension("a".to_string(), Box::new(DataType::Binary), None),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
    ]
}

#[test]
fn nulls() {
    let a = all_datatypes()
        .into_iter()
        .filter(|x| x.to_physical_type() != PhysicalType::Union)
        .all(|x| new_null_array(x, 10).null_count() == 10);
    assert!(a);

    // unions' null count is always 0
    let datatypes = vec![
        DataType::Union(vec![Field::new("a", DataType::Binary, true)], None, false),
        DataType::Union(vec![Field::new("a", DataType::Binary, true)], None, true),
    ];
    let a = all_datatypes()
        .into_iter()
        .filter(|x| x.to_physical_type() == PhysicalType::Union)
        .all(|x| new_null_array(x, 10).null_count() == 0);
    assert!(a);
}

#[test]
fn empty() {
    use DataType::*;
    let a = all_datatypes()
        .into_iter()
        .all(|x| new_empty_array(x).len() == 0);
    assert!(a);
}

#[test]
fn test_clone() {
    let a = all_datatypes()
        .into_iter()
        .all(|x| clone(new_null_array(x.clone(), 10).as_ref()) == new_null_array(x, 10));
    assert!(a);
}

#[test]
fn test_with_validity() {
    let arr = PrimitiveArray::from_slice(&[1i32, 2, 3]);
    let validity = Bitmap::from(&[true, false, true]);
    let arr = arr.with_validity(Some(validity));
    let arr_ref = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

    let expected = PrimitiveArray::from(&[Some(1i32), None, Some(3)]);
    assert_eq!(arr_ref, &expected);
}
