package ironflow

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// timeFromProto returns the zero time for an absent timestamp. AsTime on a nil
// Timestamp returns the Unix epoch, which reads as a real value.
func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// optionalTimeFromProto returns nil for an absent timestamp.
func optionalTimeFromProto(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

var timeType = reflect.TypeFor[time.Time]()

// requireTimestamps fails on a zero time.Time anywhere in a decoded result. A
// value-typed time.Time is one the server always sends; a field it can omit is
// a *time.Time, which this skips. encoding/json leaves a missing key at the zero
// time with no error, and that value reads as "no guard" where it goes back to
// the server: a zero ExpectedUpdatedAt turns the webhook guard off.
func requireTimestamps(result any) error {
	value := reflect.ValueOf(result)
	if !value.IsValid() {
		return nil
	}
	path := missingTimestamp(value, "")
	if path == "" {
		return nil
	}
	return NewError("response is missing required timestamp "+strings.TrimPrefix(path, "."), "INVALID_RESPONSE", false)
}

// missingTimestamp returns the path of the first zero time.Time, or "".
func missingTimestamp(value reflect.Value, path string) string {
	switch value.Kind() { //nolint:exhaustive // only these kinds can hold a time.Time
	case reflect.Pointer:
		if value.IsNil() || value.Type().Elem() == timeType {
			return ""
		}
		return missingTimestamp(value.Elem(), path)
	case reflect.Struct:
		if value.Type() == timeType {
			if t, ok := reflect.TypeAssert[time.Time](value); ok && t.IsZero() {
				return path
			}
			return ""
		}
		owner := path
		if owner == "" {
			owner = value.Type().Name()
		}
		for i := range value.NumField() {
			field := value.Type().Field(i)
			if !field.IsExported() || field.Tag.Get("json") == "-" {
				continue
			}
			if found := missingTimestamp(value.Field(i), owner+"."+field.Name); found != "" {
				return found
			}
		}
	case reflect.Slice, reflect.Array:
		if !mayHoldTime(value.Type().Elem()) {
			return ""
		}
		if path == "" {
			path = value.Type().Elem().Name()
		}
		for i := range value.Len() {
			if found := missingTimestamp(value.Index(i), fmt.Sprintf("%s[%d]", path, i)); found != "" {
				return found
			}
		}
	case reflect.Map:
		if !mayHoldTime(value.Type().Elem()) {
			return ""
		}
		for iter := value.MapRange(); iter.Next(); {
			if found := missingTimestamp(iter.Value(), fmt.Sprintf("%s[%v]", path, iter.Key())); found != "" {
				return found
			}
		}
	}
	return ""
}

func mayHoldTime(elem reflect.Type) bool {
	switch elem.Kind() { //nolint:exhaustive // only these kinds can hold a time.Time
	case reflect.Struct, reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		return true
	default:
		return false
	}
}
