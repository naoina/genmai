package genmai

import (
	"reflect"
	"testing"
)

func TestToCamelCase(t *testing.T) {
	for v, expected := range map[string]string{
		"genmai":   "Genmai",
		"GenmaI":   "GenmaI",
		"genma_i":  "GenmaI",
		"G_en_mai": "GEnMai",
		"g_En_maI": "GEnMaI",
	} {
		actual := ToCamelCase(v)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%v: Expect %v, but %v", v, expected, actual)
		}
	}
}

func TestToSnakeCase(t *testing.T) {
	for v, expected := range map[string]string{
		"genmai":  "genmai",
		"Genmai":  "genmai",
		"genmaI":  "genma_i",
		"gEnmAi":  "g_enm_ai",
		"gen_mai": "gen_mai",
	} {
		actual := ToSnakeCase(v)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%v: Expect %v, but %v", v, expected, actual)
		}
	}
}
