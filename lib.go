package lib

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	log "github.com/sirupsen/logrus"
)

func StrToInt(str string) int64 {
	ret, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		ret = 0
	}
	return ret
}

func StrToFloat(str string) float64 {
	ret, err := strconv.ParseFloat(str, 64)
	if err != nil {
		log.Error("Erro str to float", str, err)
		ret = 0
	}
	return ret
}

func HexToInt(str string) int64 {
	ret, err := strconv.ParseInt(str, 16, 64)
	if err != nil {
		ret = 0
	}
	return ret
}

func ByteToInt(val byte) int64 {
	ret, _ := strconv.ParseInt(string(val), 10, 64)
	return ret
}

func Lpad(s string, pad string, plength int) string {
	for i := len(s); i < plength; i++ {
		s = pad + s
	}
	return s
}

func Rpad(s string, pad string, plength int) string {
	for i := len(s); i < plength; i++ {
		s = s + pad
	}
	return s
}

func Reverse(s string) string {
	chars := []rune(s)
	for i, j := 0, len(chars)-1; i < j; i, j = i+1, j-1 {
		chars[i], chars[j] = chars[j], chars[i]
	}
	return string(chars)
}

func DateToISO(date time.Time) string {
	return date.Format("2006-01-02T15:04:05.000Z07:00")
}

func IsoToDate(date string) (time.Time, error) {
	ret, err := time.Parse("2006-01-02T15:04:05.000Z07:00", date)
	if err != nil {
		return ret, fmt.Errorf("erro convertendo data: %v", err)
	}
	return ret, nil
}

var timezonesCache map[string]*time.Location = make(map[string]*time.Location)

func IsoToLocalDate(date string, timezone string) (time.Time, error) {
	ret, err := time.Parse("2006-01-02T15:04:05.000Z07:00", date)
	if err != nil {
		return ret, fmt.Errorf("erro convertendo data: %v", err)
	}

	tz := timezonesCache[timezone]
	if tz == nil {
		tz, err = time.LoadLocation(timezone)
		if err != nil {
			log.Error("erro load location: %v", err)
		}
		timezonesCache[timezone] = tz
	}

	ret = ret.In(tz)

	return ret, nil
}

func FormatDateWithoutTZ(isoDate string, timezone string) string {
	ret, err := time.Parse("2006-01-02T15:04:05.000Z07:00", isoDate)
	if err != nil {
		log.Error("RemoveTimezone - erro parse date: %v", err)
	}

	tz := timezonesCache[timezone]
	if tz == nil {
		tz, err = time.LoadLocation(timezone)
		if err != nil {
			log.Error("erro load location: %v", err)
		}
		timezonesCache[timezone] = tz
	}

	ret = ret.In(tz)

	return ret.Format("2006-01-02 15:04:05")
}

func GetTimezoneLocation(timezone string) *time.Location {
	tz := timezonesCache[timezone]
	if tz == nil {
		tz, err := time.LoadLocation(timezone)
		if err != nil {
			log.Printf("GetTimezoneLocation - erro load location: %v", err)
		}
		timezonesCache[timezone] = tz
	}
	return tz
}

func Mean(numbers []float64) float64 {
	total := 0.0
	for _, number := range numbers {
		total += number
	}
	return total / float64(len(numbers))
}

//geo funcs

var llRegEx *regexp.Regexp = regexp.MustCompile(`(?P<lon>[0-9\-\.]+) (?P<lat>[0-9\-\.]+)`)

func StrToPolygon(str string) *s2.Polygon {
	var ps = []s2.Point{}

	matches := llRegEx.FindAllStringSubmatch(str, -1)
	for _, match := range matches {
		lon, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			log.Error("Erro Conversão long", err)
		}

		lat, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			log.Error("Erro Conversão lat", err)
		}

		// log.Debug(i, lat, lon)
		p := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lon))
		ps = append(ps, p)
	}

	loop := s2.LoopFromPoints(ps)

	//corrige erros de cadastro e inverte se for antihorario
	loop.Normalize()
	if loop.Area() > 12 {
		loop.Invert()
	}

	loops := []*s2.Loop{loop}
	pol := s2.PolygonFromLoops(loops)

	return pol
}

const earthRadiusKm float64 = 6371.01

func KmToAngle(km float64) s1.Angle {
	return s1.Angle(km / earthRadiusKm)
}

func AngleToKm(a s1.Angle) float64 {
	return a.Radians() * earthRadiusKm
}

func AngleToMeters(a s1.Angle) float64 {
	return float64(a.Radians() * earthRadiusKm * 1000)
}

func StrToPoint(str string) s2.Point {
	var p = s2.Point{}

	match := llRegEx.FindStringSubmatch(str)
	if len(match) > 2 {
		lon, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			log.Error("Erro Conversão long", err)
		}

		lat, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			log.Error("Erro Conversão lat", err)
		}
		p = s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lon))
	}

	return p
}

func PointToLatLng(p s2.Point) s2.LatLng {
	return s2.LatLngFromPoint(p)
}

func GetSpeedMetersPerSecond(datA time.Time, latA float64, lonA float64, datB time.Time, latB float64, lonB float64) float64 {
	dist := GetDistanceInMeters(latA, lonA, latB, lonB)
	secs := datB.Sub(datA).Seconds()

	speedMetersPerSecond := float64(dist) / secs

	// log.Debug("Dist:", dist, "Secs:", secs, "Speed:", speedMetersPerSecond)

	return speedMetersPerSecond
}

func GetDistanceInMeters(latA float64, lonA float64, latB float64, lonB float64) float64 {
	pa := s2.PointFromLatLng(s2.LatLngFromDegrees(latA, lonA))
	pb := s2.PointFromLatLng(s2.LatLngFromDegrees(latB, lonB))

	dist := AngleToMeters(pa.Distance(pb))

	// log.Debug("Dist:", dist)

	return dist
}

func GetDistanceAndSpeedMetersPerSecond(datA time.Time, latA float64, lonA float64, datB time.Time, latB float64, lonB float64) (float64, float64) {
	dist := GetDistanceInMeters(latA, lonA, latB, lonB)
	secs := datB.Sub(datA).Seconds()

	speedMetersPerSecond := float64(dist) / secs

	// log.Println("Dist:", dist, "Secs:", secs, "Speed:", speedMetersPerSecond)

	return dist, speedMetersPerSecond
}
