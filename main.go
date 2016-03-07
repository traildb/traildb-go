package main

import (
	"fmt"
	"time"
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
	tdb, err := Open("output.30day.29of30.tdb")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(tdb)
	fmt.Println(tdb.Version())

	// var total int
	// for i := 0; i < tdb.numTrails; i++ {
	// 	trail, err := NewTrail(tdb, i)
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
	trails, err := tdb.FindTrails(map[string]string{"type": "imp"})
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
	tdb.Close()

	// cookie := "12345678123456781234567812345678"
	// cons, err := NewTrailDBConstructor("test.tdb", "field1", "field2")
	// if err != nil {
	// 	panic(err.Error())
	// }

	// cons.Add(cookie, time.Now(), []string{"a"})
	// cons.Add(cookie, time.Now(), []string{"d", "e"})
	// cons.Add(cookie, time.Now(), []string{"e", "j"})
	// cons.Finalize()

	// cons.Close()

	// tdb, err := Open("test.tdb")
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Println(tdb)
	// fmt.Println(tdb.Version())

	// for i := 0; i < tdb.numTrails; i++ {
	// 	trail, err := NewTrail(tdb, i)
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

	// from traildb import TrailDB, TrailDBConstructor

	// cookie = '12345678123456781234567812345678'
	// cons = TrailDBConstructor('test.tdb', ['field1', 'field2'])
	// cons.add(cookie, 123, ['a'])
	// cons.add(cookie, 124, ['b', 'c'])
	// tdb = cons.finalize()

	// for cookie, trail in tdb.crumbs():
	//        print cookie, trail

}
