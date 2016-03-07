package main

import (
	"fmt"
	"time"

	"github.com/SemanticSugar/gotraildb"
)

type Ev struct {
	Timestamp int    `tdb:"timestamp"`
	Field1    string `tdb:"field1"`
	Field2    string `tdb:"field2"`
}

type RawEvent struct {
	Timestamp       int64  `tdb:"timestamp"`
	CampaignEid     string `tdb:"campaign_eid"`
	AdGroupEid      string `tdb:"adgroup_eid"`
	AdEid           string `tdb:"ad_eid"`
	AdvertisableEid string `tdb:"advertisable_eid"`
	Type            string `tdb:"type"`
	GeoCountryCode  string `tdb:"geo_country_code"`
	GeoCity         string `tdb:"geo_city"`
	GeoPostalCode   string `tdb:"geo_postal_code"`
	ConversionValue string `tdb:"conversion_value"`
	Currency        string `tdb:"currency"`
	SegmentEid      string `tdb:"segment_eid"`
	ExternalData    string `tdb:"external_data"`
	ReferrerPath    string `tdb:"referrer_path"`
	Browser         string `tdb:"browser"`
	Domain          string `tdb:"domain"`

	UtmSource   string `tdb:"utm_source"`
	UtmMedium   string `tdb:"utm_medium"`
	UtmTerm     string `tdb:"utm_term"`
	UtmContent  string `tdb:"utm_content"`
	UtmCampaign string `tdb:"utm_campaign"`

	SiteRefDomain string `tdb:"site_ref_domain"`
}

func timeTrack(start time.Time, name string) time.Time {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
	return start.Add(elapsed)
}

func main() {
	db, err := tdb.Open("output.30day.29of30.tdb")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(db)
	fmt.Println(db.Version())

	// var total int
	// for i := 0; i < db.numTrails; i++ {
	// 	trail, err := NewTrail(db, i)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	// 	// fmt.Println(trail)
	// 	for {
	// 		evt := trail.NextEvent()
	// 		if evt == nil {
	// 			trail.Close()
	// 			break
	// 		}
	// 		total++
	// 		// evt.Print()
	// 	}
	// }
	// fmt.Println(total)
	start := time.Now()
	trails, err := db.FindTrails(map[string]string{"type": "imp"})
	intermediate := timeTrack(start, "search")
	if err != nil {
		panic(err.Error())
	}
	for _, trail := range trails {
		for {
			evt := trail.NextEvent()
			if evt == nil {
				trail.Close()
				break
			}
			r := RawEvent{}
			evt.ToStruct(r)
			// evt.ToMap()
		}
	}
	_ = timeTrack(intermediate, "print")
	fmt.Println(len(trails))
	db.Close()

	cookie := "12345678123456781234567812345678"
	cons, err := tdb.NewTrailDBConstructor("test.tdb", "field1", "field2")
	if err != nil {
		panic(err.Error())
	}

	cons.Add(cookie, time.Now(), []string{"a"})
	cons.Add(cookie, time.Now(), []string{"d", "e"})
	cons.Add(cookie, time.Now(), []string{"e", "j"})
	cons.Finalize()

	cons.Close()

	// db, err := tdb.Open("test.tdb")
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Println(db)
	// fmt.Println(db.Version())

	// for i := 0; i < db.numTrails; i++ {
	// 	trail, err := tdb.NewTrail(db, i)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	// 	// fmt.Println(trail)
	// 	for {
	// 		evt := trail.NextEvent()
	// 		if evt == nil {
	// 			trail.Close()
	// 			break
	// 		}
	// 		evt.Print()
	// 		r := Ev{}
	// 		fmt.Println(evt.ToStruct(r))
	// 	}
	// }

}
