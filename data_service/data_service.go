/*
	Dataservice is a dispatcher for commands received from the ML client, to be sent then
	to the ApiServer.
	@author : KRAFESS AYYOUB
    @date : 13-07-2022
*/

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"reflect"
	pbB "stage/backend/pb"
	pb "stage/data_service/pb"
	"syscall"

	"encoding/gob"
	"time"

	"github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
)

/*
	cache data structs to be stored or loaded
*/
type DATAServer struct {
	pb.UnimplementedDATAServer
}
type StaticDataCache struct {
	CirculatingSupplyCache string
	TotalSupplyCache       string
	MaxSupplyCache         string
	DateFirstListingCache  string
	CommitsCache           string
	ForksCache             string
	StarsCache             string
	WatchingCache          string
}
type StaticDataCacheLots struct {
	NamesCache             []string
	CirculatingSupplyCache []string
	TotalSupplyCache       []string
	MaxSupplyCache         []string
	CommitsCache           []string
	ForksCache             []string
	StarsCache             []string
	WatchingCache          []string
	DateFirstListingCache  []string
}
type HistDataCache struct {
	OpenCache      []float64
	CloseCache     []float64
	HighCache      []float64
	LowCache       []float64
	MarketCapCache []float64
	VolumeCache    []float64
	DateCache      []string
}
type HistDataCacheLots struct {
	NameCache      []string
	OpenCache      [][]float64
	HighCache      [][]float64
	LowCache       [][]float64
	CloseCache     [][]float64
	MarketCapCache [][]float64
	VolumeCache    [][]float64
	DateCache      [][]string
}

var file2, _ = os.Open("serialization.gob")
var decoder = gob.NewDecoder(file2)
var cc2 = map[string]cache.Item{}
var err_cache = decoder.Decode(&cc2)
var bool_cache = false
var c = cache.NewFrom(cache.NoExpiration, 1*time.Minute, make(map[string]cache.Item))
var use_serialized_cache = true

func count(arr []string) map[string]int {
	//Creates a   dictionary of values for each element where the values are occurances in the array
	dict := make(map[string]int)
	for _, elt := range arr {
		dict[elt] = dict[elt] + 1
	}
	return dict
}

func (s *DATAServer) GetNames(ctx context.Context, request *pb.NamesRequest) (*pb.NamesResponse, error) {
	log.Println("Received a GetNames request from the ML client!")
	d, _ := os.ReadFile("cache_argument.txt")
	boolean_cache := string(d)
	if boolean_cache == "false" {
		use_serialized_cache = false
	} else {
		use_serialized_cache = true
	}
	if use_serialized_cache {
		if err_cache != nil && bool_cache != true {
			gob.Register(HistDataCache{})
			gob.Register(HistDataCacheLots{})
			gob.Register(StaticDataCache{})
			gob.Register(StaticDataCacheLots{})
			file2.Close()
			file3, _ := os.Open("serialization.gob")
			decoder3 := gob.NewDecoder(file3)
			cc3 := map[string]cache.Item{}
			_ = decoder3.Decode(&cc3)
			c = cache.NewFrom(cache.NoExpiration, 1*time.Minute, cc3)
			file3.Close()
			bool_cache = true
		}
	}
	value, found := c.Get("getNames/filter=" + request.String())
	if found {
		var final_list = []string{}
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(value)
			for i := 0; i < s.Len(); i++ {
				name_str := fmt.Sprintf("%v", s.Index(i))
				final_list = append(final_list, name_str)
			}
		}
		log.Println("Found and returned the GetNames request from cache...")
		return &pb.NamesResponse{NameDS: final_list}, nil
	} else {
		addr := "localhost:9999"
		conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			log.Fatal("Failed to establish a connection to the api server for getNames request", err)
		}
		defer conn.Close()
		client := pbB.NewAPIClient(conn)
		var numerical_filters = []pbB.NumericalFilterApi{}
		var propertites_api = []pbB.PropertyApi{}
		var values_api = []float64{}
		var categorical_filters = []string{}
		for _, elt := range request.FilterDS.NumfilterDS {
			if elt == 0 {
				numerical_filters = append(numerical_filters, pbB.NumericalFilterApi_lt)
			} else {
				numerical_filters = append(numerical_filters, pbB.NumericalFilterApi_gt)
			}
		}
		for _, elt := range request.FilterDS.PropertiesDS {
			if elt == 0 {
				propertites_api = append(propertites_api, pbB.PropertyApi_volume)
			} else if elt == 1 {
				propertites_api = append(propertites_api, pbB.PropertyApi_marketCap)
			} else if elt == 2 {
				propertites_api = append(propertites_api, pbB.PropertyApi_open)
			} else if elt == 3 {
				propertites_api = append(propertites_api, pbB.PropertyApi_close)
			} else if elt == 4 {
				propertites_api = append(propertites_api, pbB.PropertyApi_high)
			} else {
				propertites_api = append(propertites_api, pbB.PropertyApi_low)
			}
		}
		values_api = append(values_api, request.FilterDS.ValuesDS...)
		for _, elt := range request.FilterDS.CatfilterDS {
			categorical_filters = append(categorical_filters, elt.TagDS)
		}
		if len(categorical_filters) == 0 && len(numerical_filters) == 0 {
			filter_apii := pbB.FilterApi{
				NumFilterApi:  numerical_filters,
				Tag:           categorical_filters,
				PropertiesApi: propertites_api,
				ValuesApi:     values_api,
			}
			req := pbB.ListNamesRequest{
				FilterApi: &filter_apii,
			}
			res, _ := client.ListNames(context.Background(), &req)
			c.Set("getNames/filter="+request.String(), res.Name, cache.NoExpiration)
			log.Println("ApiServer's answer for getNames request well-received")
			return &pb.NamesResponse{NameDS: res.Name}, nil
		} else {
			var list_all = []string{}
			for _, elt := range categorical_filters {
				value, found := c.Get("getNames/tag=" + elt)
				if found {
					switch reflect.TypeOf(value).Kind() {
					case reflect.Slice:
						s := reflect.ValueOf(value)
						for i := 0; i < s.Len(); i++ {
							name_str := fmt.Sprintf("%v", s.Index(i))
							list_all = append(list_all, name_str)
						}
					}
				} else {
					var tags = []string{}
					tags = append(tags, elt)
					filter_tag := pbB.FilterApi{
						NumFilterApi:  []pbB.NumericalFilterApi{},
						Tag:           tags,
						PropertiesApi: []pbB.PropertyApi{},
						ValuesApi:     []float64{},
					}
					req_tag := pbB.ListNamesRequest{
						FilterApi: &filter_tag,
					}
					res, err := client.ListNames(context.Background(), &req_tag)
					if err != nil {
						log.Fatal("Failed to receive data for getNames request from the api server", err)
					}
					c.Set("getNames/tag="+elt, res.Name, cache.NoExpiration)
					list_all = append(list_all, res.Name...)
				}
			}
			for index, elt := range numerical_filters {
				value_str := fmt.Sprintf("%v", values_api[index])
				value, found := c.Get("getNames/num=" + propertites_api[index].String() + elt.String() + value_str)
				if found {
					switch reflect.TypeOf(value).Kind() {
					case reflect.Slice:
						s := reflect.ValueOf(value)
						for i := 0; i < s.Len(); i++ {
							name_str := fmt.Sprintf("%v", s.Index(i))
							list_all = append(list_all, name_str)
						}
					}
				} else {
					var numericals = []pbB.NumericalFilterApi{}
					var propreties = []pbB.PropertyApi{}
					var values = []float64{}

					numericals = append(numericals, elt)
					propreties = append(propreties, propertites_api[index])
					values = append(values, values_api[index])

					filter_num := pbB.FilterApi{
						NumFilterApi:  numericals,
						Tag:           []string{},
						PropertiesApi: propreties,
						ValuesApi:     values,
					}
					req_num := pbB.ListNamesRequest{
						FilterApi: &filter_num,
					}
					res, err := client.ListNames(context.Background(), &req_num)
					if err != nil {
						log.Fatal("Failed to receive data for getNames request from the api server", err)
					}
					c.Set("getNames/num="+propertites_api[index].String()+elt.String()+value_str, res.Name, cache.NoExpiration)
					list_all = append(list_all, res.Name...)
				}
			}
			c.Set("getNames/filter="+request.String(), list_all, cache.NoExpiration)
			log.Println("ApiServer's answer for getNames request well-received")
			return &pb.NamesResponse{NameDS: list_all}, nil
		}
	}
}

func (s *DATAServer) GetStat(ctx context.Context, request *pb.StatRequest) (*pb.StatResponse, error) {
	log.Println("Received a GetStat request from the ML client!")
	d, _ := os.ReadFile("cache_argument.txt")
	boolean_cache := string(d)
	if boolean_cache == "false" {
		use_serialized_cache = false
	} else {
		use_serialized_cache = true
	}
	if use_serialized_cache {
		if err_cache != nil && bool_cache != true {
			gob.Register(HistDataCache{})
			gob.Register(HistDataCacheLots{})
			gob.Register(StaticDataCache{})
			gob.Register(StaticDataCacheLots{})
			file2.Close()
			file3, _ := os.Open("serialization.gob")
			decoder3 := gob.NewDecoder(file3)
			cc3 := map[string]cache.Item{}
			_ = decoder3.Decode(&cc3)
			c = cache.NewFrom(cache.NoExpiration, 1*time.Minute, cc3)
			file3.Close()
			bool_cache = true
		}
	}
	value, found := c.Get("getStat/filter=" + request.String())
	if found {
		v := reflect.ValueOf(value).Interface().(StaticDataCacheLots)
		result := pb.StatResponse{
			NameDS:              v.NamesCache,
			CirculatingSupplyDS: v.CirculatingSupplyCache,
			TotalSupplyDS:       v.TotalSupplyCache,
			MaxSupplyDS:         v.MaxSupplyCache,
			CommitsDS:           v.CommitsCache,
			ForksDS:             v.ForksCache,
			StarsDS:             v.StarsCache,
			WatchingDS:          v.WatchingCache,
			DateFirstListingDS:  v.DateFirstListingCache,
		}
		log.Println("Found the cryptos static data stored in the cache and returned it...")
		return &result, nil
	} else {
		addr := "localhost:9999"
		conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			log.Fatal("Failed to establish a connection to the api server for getStat request", err)
		}
		defer conn.Close()
		client := pbB.NewAPIClient(conn)
		var numerical_filters = []pbB.NumericalFilterApi{}
		var propertites_api = []pbB.PropertyApi{}
		var values_api = []float64{}
		var categorical_filters = []string{}
		for _, elt := range request.FilterDS.NumfilterDS {
			if elt == 0 {
				numerical_filters = append(numerical_filters, pbB.NumericalFilterApi_lt)
			} else {
				numerical_filters = append(numerical_filters, pbB.NumericalFilterApi_gt)
			}
		}
		for _, elt := range request.FilterDS.PropertiesDS {
			if elt == 0 {
				propertites_api = append(propertites_api, pbB.PropertyApi_volume)
			} else if elt == 1 {
				propertites_api = append(propertites_api, pbB.PropertyApi_marketCap)
			} else if elt == 2 {
				propertites_api = append(propertites_api, pbB.PropertyApi_open)
			} else if elt == 3 {
				propertites_api = append(propertites_api, pbB.PropertyApi_close)
			} else if elt == 4 {
				propertites_api = append(propertites_api, pbB.PropertyApi_high)
			} else {
				propertites_api = append(propertites_api, pbB.PropertyApi_low)
			}
		}
		values_api = append(values_api, request.FilterDS.ValuesDS...)
		for _, elt := range request.FilterDS.CatfilterDS {
			categorical_filters = append(categorical_filters, elt.TagDS)
		}
		filter_api := pbB.FilterApi{
			NumFilterApi:  numerical_filters,
			Tag:           categorical_filters,
			PropertiesApi: propertites_api,
			ValuesApi:     values_api,
		}
		req := pbB.ListNamesRequest{FilterApi: &filter_api}
		res, err := client.ListNames(context.Background(), &req)
		if err != nil {
			log.Fatal("Failed to get the names for the appropriate filter ...")
		}
		number_of_filters := len(request.FilterDS.CatfilterDS) + len(request.FilterDS.NumfilterDS)
		if number_of_filters == 0 {
			number_of_filters = 1
		}
		var list_names = []string{}
		var list_circulating_supply = []string{}
		var list_total_supply = []string{}
		var list_max_supply = []string{}
		var list_commits = []string{}
		var list_forks = []string{}
		var list_stars = []string{}
		var list_watching = []string{}
		var list_date_first_listing = []string{}
		dict := count(res.Name)
		for key, value := range dict {
			if value == number_of_filters {
				list_names = append(list_names, key)
			}
		}
		/*
				number_of_dividers := 10
				number_of_cryptos := len(list_names)
				number_of_workers := number_of_cryptos / number_of_dividers
				number_of_remainders := number_of_cryptos % number_of_dividers
				if number_of_remainders != 0 {
					number_of_workers = number_of_workers + 1
				}
			var wg sync.WaitGroup
			wg.Add(len(list_names))
		*/
		for i := 0; i < len(list_names); i++ {
			/*
				go func(i int) {
					defer wg.Done()

						var aux_list_names = []string{}
						if i == number_of_workers-1 {
							if number_of_remainders != 0 {
								aux_list_names = list_names[i*number_of_dividers:]
							} else {
								aux_list_names = list_names[i*number_of_dividers : (i+1)*number_of_dividers]
							}
						} else {
							aux_list_names = list_names[i*number_of_dividers : (i+1)*number_of_dividers]
						}
						for k := 0; k < len(aux_list_names); k++ {

							request_stat := pbB.StaticDataRequest{Name: aux_list_names[i]}
							value, found := c.Get("Stat/" + aux_list_names[i])
			*/
			client := pbB.NewAPIClient(conn)
			request_stat := pbB.StaticDataRequest{Name: list_names[i]}
			value, found := c.Get("Stat/" + list_names[i])
			if found {
				v := reflect.ValueOf(value).Interface().(StaticDataCache)
				list_circulating_supply = append(list_circulating_supply, v.CirculatingSupplyCache)
				list_total_supply = append(list_total_supply, v.TotalSupplyCache)
				list_max_supply = append(list_max_supply, v.MaxSupplyCache)
				list_commits = append(list_commits, v.CommitsCache)
				list_forks = append(list_forks, v.ForksCache)
				list_stars = append(list_stars, v.StarsCache)
				list_watching = append(list_watching, v.WatchingCache)
				list_date_first_listing = append(list_date_first_listing, v.DateFirstListingCache)
			} else {
				res, _ := client.GetStaticData(context.Background(), &request_stat)
				list_circulating_supply = append(list_circulating_supply, res.CirculatingSupply)
				list_total_supply = append(list_total_supply, res.TotalSupply)
				list_max_supply = append(list_max_supply, res.MaxSupply)
				list_commits = append(list_commits, *res.Commits)
				list_forks = append(list_forks, *res.Forks)
				list_stars = append(list_stars, *res.Stars)
				list_watching = append(list_watching, *res.Watching)
				list_date_first_listing = append(list_date_first_listing, res.DateFirstListing)
				c.Set("Stat/"+list_names[i], StaticDataCache{CirculatingSupplyCache: res.CirculatingSupply,
					TotalSupplyCache: res.TotalSupply, MaxSupplyCache: res.MaxSupply, CommitsCache: *res.Commits, ForksCache: *res.Forks,
					StarsCache: *res.Stars, WatchingCache: *res.Watching, DateFirstListingCache: res.DateFirstListing}, cache.NoExpiration)
			}
			/*
						}
				}(i)
			*/
		}
		/*
			wg.Wait()
		*/
		all_data := StaticDataCacheLots{
			NamesCache:             list_names,
			CirculatingSupplyCache: list_circulating_supply,
			TotalSupplyCache:       list_total_supply,
			MaxSupplyCache:         list_max_supply,
			CommitsCache:           list_commits,
			ForksCache:             list_forks,
			StarsCache:             list_stars,
			WatchingCache:          list_watching,
			DateFirstListingCache:  list_date_first_listing,
		}
		result := pb.StatResponse{
			NameDS:              list_names,
			CirculatingSupplyDS: list_circulating_supply,
			TotalSupplyDS:       list_total_supply,
			MaxSupplyDS:         list_max_supply,
			CommitsDS:           list_commits,
			ForksDS:             list_forks,
			StarsDS:             list_stars,
			WatchingDS:          list_watching,
			DateFirstListingDS:  list_date_first_listing,
		}
		c.Set("getStat/filter="+request.String(), all_data, cache.NoExpiration)
		log.Println("ApiServer's answer for getStat request well-received")
		return &result, nil
	}
}

func (s *DATAServer) GetHist(ctx context.Context, request *pb.HistRequest) (*pb.HistResponse, error) {
	log.Println("Received a GetHist request from the ML client!")
	d, _ := os.ReadFile("cache_argument.txt")
	boolean_cache := string(d)
	if boolean_cache == "false" {
		use_serialized_cache = false
	} else {
		use_serialized_cache = true
	}
	if use_serialized_cache {
		if err_cache != nil && bool_cache != true {
			gob.Register(HistDataCache{})
			gob.Register(HistDataCacheLots{})
			gob.Register(StaticDataCache{})
			gob.Register(StaticDataCacheLots{})
			file2.Close()
			file3, _ := os.Open("serialization.gob")
			decoder3 := gob.NewDecoder(file3)
			cc3 := map[string]cache.Item{}
			_ = decoder3.Decode(&cc3)
			c = cache.NewFrom(cache.NoExpiration, 1*time.Minute, cc3)
			file3.Close()
			bool_cache = true
		}
	}
	value, found := c.Get("getHist/filter=" + request.String())

	if found {
		v := reflect.ValueOf(value).Interface().(HistDataCacheLots)
		var list_open = []*pb.ListOpenPrice{}
		for k := 0; k < len(v.OpenCache); k++ {
			aux_list_open := pb.ListOpenPrice{OpenPrice: v.OpenCache[k]}
			list_open = append(list_open, &aux_list_open)
		}
		var list_close = []*pb.ListClosePrice{}
		for k := 0; k < len(v.CloseCache); k++ {
			aux_list_close := pb.ListClosePrice{ClosePrice: v.CloseCache[k]}
			list_close = append(list_close, &aux_list_close)
		}
		var list_high = []*pb.ListHighPrice{}
		for k := 0; k < len(v.HighCache); k++ {
			aux_list_high := pb.ListHighPrice{HighPrice: v.HighCache[k]}
			list_high = append(list_high, &aux_list_high)
		}
		var list_low = []*pb.ListLowPrice{}
		for k := 0; k < len(v.LowCache); k++ {
			aux_list_low := pb.ListLowPrice{LowPrice: v.LowCache[k]}
			list_low = append(list_low, &aux_list_low)
		}
		var list_volume = []*pb.ListVolume{}
		for k := 0; k < len(v.VolumeCache); k++ {
			aux_list_volume := pb.ListVolume{Volume: v.VolumeCache[k]}
			list_volume = append(list_volume, &aux_list_volume)
		}
		var list_marketCap = []*pb.ListMarketCap{}
		for k := 0; k < len(v.MarketCapCache); k++ {
			aux_list_marketcap := pb.ListMarketCap{MarketCap: v.MarketCapCache[k]}
			list_marketCap = append(list_marketCap, &aux_list_marketcap)
		}
		var list_dates = []*pb.ListDates{}
		for k := 0; k < len(v.DateCache); k++ {
			aux_list_dates := pb.ListDates{Date: v.DateCache[k]}
			list_dates = append(list_dates, &aux_list_dates)
		}
		result := pb.HistResponse{
			NameDS:         v.NameCache,
			ListOpenPrice:  list_open,
			ListClosePrice: list_close,
			ListHighPrice:  list_high,
			ListLowPrice:   list_low,
			VolumeDS:       list_volume,
			MarketCapDS:    list_marketCap,
			DateDS:         list_dates,
		}
		log.Println("Found the response stored in the cache and returned it...")
		return &result, nil

	} else {
		addr := "localhost:9999"
		conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			log.Fatal("Failed to establish a connection to the api server for getHist request", err)
		}
		defer conn.Close()
		client := pbB.NewAPIClient(conn)
		var numerical_filters = []pbB.NumericalFilterApi{}
		var propertites_api = []pbB.PropertyApi{}
		var values_api = []float64{}
		var categorical_filters = []string{}
		for _, elt := range request.FilterDS.NumfilterDS {
			if elt == 0 {
				numerical_filters = append(numerical_filters, pbB.NumericalFilterApi_lt)
			} else {
				numerical_filters = append(numerical_filters, pbB.NumericalFilterApi_gt)
			}
		}
		for _, elt := range request.FilterDS.PropertiesDS {
			if elt == 0 {
				propertites_api = append(propertites_api, pbB.PropertyApi_volume)
			} else if elt == 1 {
				propertites_api = append(propertites_api, pbB.PropertyApi_marketCap)
			} else if elt == 2 {
				propertites_api = append(propertites_api, pbB.PropertyApi_open)
			} else if elt == 3 {
				propertites_api = append(propertites_api, pbB.PropertyApi_close)
			} else if elt == 4 {
				propertites_api = append(propertites_api, pbB.PropertyApi_high)
			} else {
				propertites_api = append(propertites_api, pbB.PropertyApi_low)
			}
		}
		values_api = append(values_api, request.FilterDS.ValuesDS...)
		for _, elt := range request.FilterDS.CatfilterDS {
			categorical_filters = append(categorical_filters, elt.TagDS)
		}
		filter_api := pbB.FilterApi{
			NumFilterApi:  numerical_filters,
			Tag:           categorical_filters,
			PropertiesApi: propertites_api,
			ValuesApi:     values_api,
		}
		req := pbB.ListNamesRequest{FilterApi: &filter_api}
		res, err := client.ListNames(context.Background(), &req)
		if err != nil {
			log.Fatal("Failed to get the names for the appropriate filter ...")
		}
		number_of_filters := len(request.FilterDS.CatfilterDS) + len(request.FilterDS.NumfilterDS)
		if number_of_filters == 0 {
			number_of_filters = 1
		}
		var list_names = []string{}
		var list_open_price = [][]float64{}
		var list_close_price = [][]float64{}
		var list_high_price = [][]float64{}
		var list_low_price = [][]float64{}
		var list_volume = [][]float64{}
		var list_marketcap = [][]float64{}
		var list_dates = [][]string{}
		dict := count(res.Name)
		for key, value := range dict {
			if value == number_of_filters {
				list_names = append(list_names, key)
			}
		}
		/*
				number_of_dividers := 10
				number_of_cryptos := len(list_names)
				number_of_workers := number_of_cryptos / number_of_dividers
				number_of_remainders := number_of_cryptos % number_of_dividers
				if number_of_remainders != 0 {
					number_of_workers = number_of_workers + 1
				}
			var wg sync.WaitGroup
			wg.Add(1)
			for i := 0; i < len(list_names); i++ {
				go func(i int) {
					defer wg.Done()
					/*
						var aux_list_names = []string{}
						if i == number_of_workers-1 {
							if number_of_remainders != 0 {
								aux_list_names = list_names[i*number_of_dividers:]
							} else {
								aux_list_names = list_names[i*number_of_dividers : (i+1)*number_of_dividers]
							}
						} else {
							aux_list_names = list_names[i*number_of_dividers : (i+1)*number_of_dividers]
						}
						for k := 0; k < len(aux_list_names); k++ {
		*/
		for i := 0; i < len(list_names); i++ {
			value, found := c.Get("Price/" + list_names[i])
			if found {

				v := reflect.ValueOf(value).Interface().(HistDataCache)
				end_date, _ := time.Parse("2006-01-02", v.DateCache[0][:10])
				start_date, _ := time.Parse("2006-01-02", v.DateCache[len(v.DateCache)-1][:10])
				start_date_request, _ := time.Parse("2006-01-02", request.StartDateDS)
				end_date_request, _ := time.Parse("2006-01-02", request.EndDateDS)

				if (start_date_request.After(start_date) || start_date_request.Equal(start_date)) && (end_date_request.Before(end_date) || end_date_request.Equal(end_date)) {
					diff1 := start_date_request.Sub(start_date)
					diff2 := end_date.Sub(end_date_request)
					j1 := int(diff1.Hours() / 24)
					j2 := int(diff2.Hours() / 24)
					list_open_price = append(list_open_price, v.OpenCache[j2:len(v.DateCache)-j1])
					list_close_price = append(list_close_price, v.CloseCache[j2:len(v.DateCache)-j1])
					list_high_price = append(list_high_price, v.HighCache[j2:len(v.DateCache)-j1])
					list_low_price = append(list_low_price, v.LowCache[j2:len(v.DateCache)-j1])
					list_dates = append(list_dates, v.DateCache[j2:len(v.DateCache)-j1])
					list_volume = append(list_volume, v.VolumeCache[j2:len(v.VolumeCache)-j1])
					list_marketcap = append(list_marketcap, v.MarketCapCache[j2:len(v.MarketCapCache)-j1])

				} else if (start_date_request.Before(start_date) || start_date_request.Equal(start_date)) && (end_date_request.After(end_date) || end_date_request.Equal(end_date)) {
					request1 := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: v.DateCache[0],
						EndDateHist:    request.EndDateDS,
					}
					request2 := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: request.StartDateDS,
						EndDateHist:    v.DateCache[len(v.DateCache)-1],
					}
					result1, _ := client.GetPrice(context.Background(), &request1)
					result2, _ := client.GetPrice(context.Background(), &request2)
					var all_openPrice = []float64{}
					all_openPrice = append(all_openPrice, result1.OpenPrice[:len(result1.DatePrice)-1]...)
					all_openPrice = append(all_openPrice, v.OpenCache...)
					all_openPrice = append(all_openPrice, result2.OpenPrice[1:]...)
					var all_closePrice = []float64{}
					all_closePrice = append(all_closePrice, result1.ClosePrice[:len(result1.DatePrice)-1]...)
					all_closePrice = append(all_closePrice, v.CloseCache...)
					all_closePrice = append(all_closePrice, result2.ClosePrice[1:]...)
					var all_highPrice = []float64{}
					all_highPrice = append(all_highPrice, result1.HighPrice[:len(result1.DatePrice)-1]...)
					all_highPrice = append(all_highPrice, v.HighCache...)
					all_highPrice = append(all_highPrice, result2.HighPrice[1:]...)
					var all_lowPrice = []float64{}
					all_lowPrice = append(all_lowPrice, result1.LowPrice[:len(result1.DatePrice)-1]...)
					all_lowPrice = append(all_lowPrice, v.LowCache...)
					all_lowPrice = append(all_lowPrice, result2.LowPrice[1:]...)
					var all_date = []string{}
					all_date = append(all_date, result1.DatePrice[:len(result1.DatePrice)-1]...)
					all_date = append(all_date, v.DateCache...)
					all_date = append(all_date, result2.DatePrice[1:]...)
					var all_volume = []float64{}
					all_volume = append(all_volume, result1.Volume[:len(result1.Volume)-1]...)
					all_volume = append(all_volume, v.VolumeCache...)
					all_volume = append(all_volume, result2.Volume[1:]...)
					var all_marketCap = []float64{}
					all_marketCap = append(all_marketCap, result1.MarketCap[:len(result1.MarketCap)-1]...)
					all_marketCap = append(all_marketCap, v.MarketCapCache...)
					all_marketCap = append(all_marketCap, result2.MarketCap[1:]...)
					to_be_cached := HistDataCache{
						OpenCache:      all_openPrice,
						CloseCache:     all_closePrice,
						HighCache:      all_highPrice,
						LowCache:       all_lowPrice,
						VolumeCache:    all_volume,
						MarketCapCache: all_marketCap,
						DateCache:      all_date,
					}
					list_open_price = append(list_open_price, all_openPrice)
					list_close_price = append(list_close_price, all_closePrice)
					list_high_price = append(list_high_price, all_highPrice)
					list_low_price = append(list_low_price, all_lowPrice)
					list_dates = append(list_dates, all_date)
					list_volume = append(list_volume, all_volume)
					list_marketcap = append(list_marketcap, all_marketCap)
					c.Set("Price/"+list_names[i], to_be_cached, cache.NoExpiration)

				} else if (start_date_request.After(start_date) || start_date_request.Equal(start_date)) && (end_date_request.After(end_date) || end_date_request.Equal(end_date)) {
					request3 := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: v.DateCache[0],
						EndDateHist:    request.EndDateDS,
					}
					diff := start_date_request.Sub(end_date)
					j := int(diff.Hours() / 24)
					result, _ := client.GetPrice(context.Background(), &request3)
					aux_open := v.OpenCache[:j+1]
					aux_close := v.CloseCache[:j+1]
					aux_high := v.HighCache[:j+1]
					aux_low := v.LowCache[:j+1]
					aux_date := v.DateCache[:j+1]
					aux_volume := v.VolumeCache[:j+1]
					aux_marketcap := v.MarketCapCache[:j+1]
					var all_openPrice = []float64{}
					all_openPrice = append(all_openPrice, result.OpenPrice...)
					all_openPrice = append(all_openPrice, v.OpenCache...)
					var all_closePrice = []float64{}
					all_closePrice = append(all_closePrice, result.ClosePrice...)
					all_closePrice = append(all_closePrice, v.CloseCache...)
					var all_highPrice = []float64{}
					all_highPrice = append(all_highPrice, result.HighPrice...)
					all_highPrice = append(all_highPrice, v.HighCache...)
					var all_lowPrice = []float64{}
					all_lowPrice = append(all_lowPrice, result.LowPrice...)
					all_lowPrice = append(all_lowPrice, v.LowCache...)
					var all_date = []string{}
					all_date = append(all_date, result.DatePrice...)
					all_date = append(all_date, v.DateCache...)
					var all_volume = []float64{}
					all_volume = append(all_volume, result.Volume...)
					all_volume = append(all_volume, v.VolumeCache...)
					var all_marketcap = []float64{}
					all_marketcap = append(all_marketcap, result.MarketCap...)
					all_marketcap = append(all_marketcap, v.MarketCapCache...)
					to_be_cached := HistDataCache{
						OpenCache:      all_openPrice,
						CloseCache:     all_closePrice,
						HighCache:      all_highPrice,
						LowCache:       all_lowPrice,
						VolumeCache:    all_volume,
						MarketCapCache: all_marketcap,
						DateCache:      all_date,
					}
					var final_list_open = []float64{}
					final_list_open = append(final_list_open, result.OpenPrice...)
					final_list_open = append(final_list_open, aux_open...)
					var final_list_close = []float64{}
					final_list_close = append(final_list_close, result.ClosePrice...)
					final_list_close = append(final_list_close, aux_close...)
					var final_list_high = []float64{}
					final_list_high = append(final_list_high, result.HighPrice...)
					final_list_high = append(final_list_high, aux_high...)
					var final_list_low = []float64{}
					final_list_low = append(final_list_low, result.LowPrice...)
					final_list_low = append(final_list_low, aux_low...)
					var final_list_date = []string{}
					final_list_date = append(final_list_date, result.DatePrice...)
					final_list_date = append(final_list_date, aux_date...)
					var final_list_volume = []float64{}
					final_list_volume = append(final_list_volume, result.Volume...)
					final_list_volume = append(final_list_volume, aux_volume...)
					var final_list_marketcap = []float64{}
					final_list_marketcap = append(final_list_marketcap, result.MarketCap...)
					final_list_marketcap = append(final_list_marketcap, aux_marketcap...)
					c.Set("Price/"+list_names[i], to_be_cached, cache.NoExpiration)
					list_open_price = append(list_open_price, final_list_open)
					list_close_price = append(list_close_price, final_list_close)
					list_high_price = append(list_high_price, final_list_high)
					list_low_price = append(list_low_price, final_list_low)
					list_volume = append(list_volume, final_list_volume)
					list_marketcap = append(list_marketcap, final_list_marketcap)
					list_dates = append(list_dates, final_list_date)

				} else if start_date_request.After(end_date) || start_date_request.Equal(end_date) {
					req := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: request.StartDateDS,
						EndDateHist:    request.EndDateDS,
					}
					req2 := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: v.DateCache[0],
						EndDateHist:    req.StartDateGHist,
					}
					res, err := client.GetPrice(context.Background(), &req)
					res2, err := client.GetPrice(context.Background(), &req2)
					if err != nil {
						log.Fatal("Failed to receive data for getPrice request from the api server", err)
					}
					list_open_price = append(list_open_price, res.OpenPrice)
					list_close_price = append(list_close_price, res.ClosePrice)
					list_high_price = append(list_high_price, res.HighPrice)
					list_low_price = append(list_low_price, res.LowPrice)
					list_volume = append(list_volume, res.Volume)
					list_marketcap = append(list_marketcap, res.MarketCap)
					list_dates = append(list_dates, res.DatePrice)
					var all_open_price = []float64{}
					all_open_price = append(all_open_price, res.OpenPrice...)
					all_open_price = append(all_open_price, res2.OpenPrice...)
					all_open_price = append(all_open_price, v.OpenCache...)
					var all_close_price = []float64{}
					all_close_price = append(all_close_price, res.ClosePrice...)
					all_close_price = append(all_close_price, res2.ClosePrice...)
					all_close_price = append(all_close_price, v.CloseCache...)
					var all_high_price = []float64{}
					all_high_price = append(all_high_price, res.HighPrice...)
					all_high_price = append(all_high_price, res2.HighPrice...)
					all_high_price = append(all_high_price, v.HighCache...)
					var all_low_price = []float64{}
					all_low_price = append(all_low_price, res.LowPrice...)
					all_low_price = append(all_low_price, res2.LowPrice...)
					all_low_price = append(all_low_price, v.LowCache...)
					var all_volumes = []float64{}
					all_volumes = append(all_volumes, res.Volume...)
					all_volumes = append(all_volumes, res2.Volume...)
					all_volumes = append(all_volumes, v.VolumeCache...)
					var all_marketcaps = []float64{}
					all_marketcaps = append(all_marketcaps, res.MarketCap...)
					all_marketcaps = append(all_marketcaps, res2.MarketCap...)
					all_marketcaps = append(all_marketcaps, v.MarketCapCache...)
					var all_dates = []string{}
					all_dates = append(all_dates, res.DatePrice...)
					all_dates = append(all_dates, res2.DatePrice...)
					all_dates = append(all_dates, v.DateCache...)
					to_be_cached := HistDataCache{
						OpenCache:      all_open_price,
						CloseCache:     all_close_price,
						HighCache:      all_high_price,
						LowCache:       all_low_price,
						VolumeCache:    all_volumes,
						MarketCapCache: all_marketcaps,
						DateCache:      all_dates,
					}
					c.Set("Price/"+list_names[i], to_be_cached, cache.NoExpiration)

				} else if end_date_request.Before(start_date) || end_date_request.Equal(start_date) {
					req := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: request.StartDateDS,
						EndDateHist:    request.EndDateDS,
					}
					req2 := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: request.EndDateDS,
						EndDateHist:    v.DateCache[len(v.DateCache)-1],
					}
					res, _ := client.GetPrice(context.Background(), &req)
					res2, err := client.GetPrice(context.Background(), &req2)
					if err != nil {
						log.Fatal("Failed to receive data for getPrice request from the api server", err)
					}
					list_open_price = append(list_open_price, res.OpenPrice)
					list_close_price = append(list_close_price, res.ClosePrice)
					list_high_price = append(list_high_price, res.HighPrice)
					list_low_price = append(list_low_price, res.LowPrice)
					list_volume = append(list_volume, res.Volume)
					list_marketcap = append(list_marketcap, res.MarketCap)
					list_dates = append(list_dates, res.DatePrice)
					var all_open_price = []float64{}
					all_open_price = append(all_open_price, v.OpenCache...)
					all_open_price = append(all_open_price, res2.OpenPrice...)
					all_open_price = append(all_open_price, res.OpenPrice...)
					var all_close_price = []float64{}
					all_close_price = append(all_close_price, v.CloseCache...)
					all_close_price = append(all_close_price, res2.ClosePrice...)
					all_close_price = append(all_close_price, res.ClosePrice...)
					var all_high_price = []float64{}
					all_high_price = append(all_high_price, v.HighCache...)
					all_high_price = append(all_high_price, res2.HighPrice...)
					all_high_price = append(all_high_price, res.HighPrice...)
					var all_low_price = []float64{}
					all_low_price = append(all_low_price, v.LowCache...)
					all_low_price = append(all_low_price, res2.LowPrice...)
					all_low_price = append(all_low_price, res.LowPrice...)
					var all_volumes = []float64{}
					all_volumes = append(all_volumes, v.VolumeCache...)
					all_volumes = append(all_volumes, res2.Volume...)
					all_volumes = append(all_volumes, res.Volume...)
					var all_marketcaps = []float64{}
					all_marketcaps = append(all_marketcaps, v.MarketCapCache...)
					all_marketcaps = append(all_marketcaps, res2.MarketCap...)
					all_marketcaps = append(all_marketcaps, res.MarketCap...)
					var all_dates = []string{}
					all_dates = append(all_dates, v.DateCache...)
					all_dates = append(all_dates, res2.DatePrice...)
					all_dates = append(all_dates, res.DatePrice...)
					to_be_cached := HistDataCache{
						OpenCache:      all_open_price,
						CloseCache:     all_close_price,
						HighCache:      all_high_price,
						LowCache:       all_low_price,
						VolumeCache:    all_volumes,
						MarketCapCache: all_marketcaps,
						DateCache:      all_dates,
					}
					c.Set("Price/"+list_names[i], to_be_cached, cache.NoExpiration)

				} else {
					request3 := pbB.PriceRequest{
						Name:           list_names[i],
						StartDateGHist: request.StartDateDS,
						EndDateHist:    v.DateCache[len(v.DateCache)-1],
					}
					diff := end_date_request.Sub(start_date)
					j := int(diff.Hours() / 24)
					result, _ := client.GetPrice(context.Background(), &request3)
					aux_open := v.OpenCache[j:]
					aux_close := v.CloseCache[j:]
					aux_high := v.HighCache[j:]
					aux_low := v.LowCache[j:]
					aux_date := v.DateCache[j:]
					aux_volume := v.VolumeCache[j:]
					aux_marketcap := v.MarketCapCache[j:]
					var all_openPrice = []float64{}
					all_openPrice = append(all_openPrice, v.OpenCache...)
					all_openPrice = append(all_openPrice, result.OpenPrice...)
					var all_closePrice = []float64{}
					all_closePrice = append(all_closePrice, v.CloseCache...)
					all_closePrice = append(all_closePrice, result.ClosePrice...)
					var all_highPrice = []float64{}
					all_highPrice = append(all_highPrice, v.HighCache...)
					all_highPrice = append(all_highPrice, result.HighPrice...)
					var all_lowPrice = []float64{}
					all_lowPrice = append(all_lowPrice, v.LowCache...)
					all_lowPrice = append(all_lowPrice, result.LowPrice...)
					var all_date = []string{}
					all_date = append(all_date, v.DateCache...)
					all_date = append(all_date, result.DatePrice...)
					var all_volume = []float64{}
					all_volume = append(all_volume, v.VolumeCache...)
					all_volume = append(all_volume, result.Volume...)
					var all_marketCap = []float64{}
					all_marketCap = append(all_marketCap, v.MarketCapCache...)
					all_marketCap = append(all_marketCap, result.MarketCap...)
					to_be_cached := HistDataCache{
						OpenCache:      all_openPrice,
						CloseCache:     all_closePrice,
						HighCache:      all_highPrice,
						LowCache:       all_lowPrice,
						VolumeCache:    all_volume,
						MarketCapCache: all_marketCap,
						DateCache:      all_date,
					}
					var final_list_open = []float64{}
					final_list_open = append(final_list_open, aux_open...)
					final_list_open = append(final_list_open, result.OpenPrice...)
					var final_list_close = []float64{}
					final_list_close = append(final_list_close, aux_close...)
					final_list_close = append(final_list_close, result.ClosePrice...)
					var final_list_high = []float64{}
					final_list_high = append(final_list_high, aux_high...)
					final_list_high = append(final_list_high, result.HighPrice...)
					var final_list_low = []float64{}
					final_list_low = append(final_list_low, aux_low...)
					final_list_low = append(final_list_low, result.LowPrice...)
					var final_list_date = []string{}
					final_list_date = append(final_list_date, aux_date...)
					final_list_date = append(final_list_date, result.DatePrice...)
					var final_list_volume = []float64{}
					final_list_volume = append(final_list_volume, aux_volume...)
					final_list_volume = append(final_list_volume, result.Volume...)
					var final_list_marketcap = []float64{}
					final_list_marketcap = append(final_list_marketcap, aux_marketcap...)
					final_list_marketcap = append(final_list_marketcap, result.MarketCap...)
					c.Set("Price/"+list_names[i], to_be_cached, cache.NoExpiration)
					list_open_price = append(list_open_price, final_list_open)
					list_close_price = append(list_close_price, final_list_close)
					list_high_price = append(list_high_price, final_list_high)
					list_low_price = append(list_low_price, final_list_low)
					list_volume = append(list_volume, final_list_volume)
					list_marketcap = append(list_marketcap, final_list_marketcap)
					list_dates = append(list_dates, final_list_date)
				}

			} else {

				req := pbB.PriceRequest{
					Name:           list_names[i],
					StartDateGHist: request.StartDateDS,
					EndDateHist:    request.EndDateDS,
				}
				res, err := client.GetPrice(context.Background(), &req)
				if err != nil {
					log.Fatal("Failed to receive data for getPrice request from the api server in here", err)
				}
				to_be_cached := HistDataCache{
					OpenCache:      res.OpenPrice,
					CloseCache:     res.ClosePrice,
					HighCache:      res.HighPrice,
					LowCache:       res.LowPrice,
					VolumeCache:    res.Volume,
					MarketCapCache: res.MarketCap,
					DateCache:      res.DatePrice,
				}
				c.Set("Price/"+req.Name, to_be_cached, cache.NoExpiration)
				list_open_price = append(list_open_price, res.OpenPrice)
				list_close_price = append(list_close_price, res.ClosePrice)
				list_high_price = append(list_high_price, res.HighPrice)
				list_low_price = append(list_low_price, res.LowPrice)
				list_volume = append(list_volume, res.Volume)
				list_marketcap = append(list_marketcap, res.MarketCap)
				list_dates = append(list_dates, res.DatePrice)
			}
		}
		var list_open = []*pb.ListOpenPrice{}
		for k := 0; k < len(list_open_price); k++ {
			aux_list_open := pb.ListOpenPrice{OpenPrice: list_open_price[k]}
			list_open = append(list_open, &aux_list_open)
		}
		var list_close = []*pb.ListClosePrice{}
		for k := 0; k < len(list_close_price); k++ {
			aux_list_close := pb.ListClosePrice{ClosePrice: list_close_price[k]}
			list_close = append(list_close, &aux_list_close)
		}
		var list_high = []*pb.ListHighPrice{}
		for k := 0; k < len(list_high_price); k++ {
			aux_list_high := pb.ListHighPrice{HighPrice: list_high_price[k]}
			list_high = append(list_high, &aux_list_high)
		}
		var list_low = []*pb.ListLowPrice{}
		for k := 0; k < len(list_low_price); k++ {
			aux_list_low := pb.ListLowPrice{LowPrice: list_low_price[k]}
			list_low = append(list_low, &aux_list_low)
		}
		var list_volume_trading = []*pb.ListVolume{}
		for k := 0; k < len(list_volume); k++ {
			aux_list_volume := pb.ListVolume{Volume: list_volume[k]}
			list_volume_trading = append(list_volume_trading, &aux_list_volume)
		}
		var list_marketCap = []*pb.ListMarketCap{}
		for k := 0; k < len(list_marketcap); k++ {
			aux_list_marketcap := pb.ListMarketCap{MarketCap: list_marketcap[k]}
			list_marketCap = append(list_marketCap, &aux_list_marketcap)
		}
		var list_dates_hist = []*pb.ListDates{}
		for k := 0; k < len(list_dates); k++ {
			aux_list_dates := pb.ListDates{Date: list_dates[k]}
			list_dates_hist = append(list_dates_hist, &aux_list_dates)
		}
		result := pb.HistResponse{
			NameDS:         list_names,
			ListOpenPrice:  list_open,
			ListClosePrice: list_close,
			ListHighPrice:  list_high,
			ListLowPrice:   list_low,
			VolumeDS:       list_volume_trading,
			MarketCapDS:    list_marketCap,
			DateDS:         list_dates_hist,
		}
		to_be_cached := HistDataCacheLots{
			NameCache:      list_names,
			OpenCache:      list_open_price,
			HighCache:      list_high_price,
			LowCache:       list_low_price,
			CloseCache:     list_close_price,
			MarketCapCache: list_marketcap,
			VolumeCache:    list_volume,
			DateCache:      list_dates,
		}
		c.Set("getHist/filter="+request.String(), to_be_cached, cache.NoExpiration)
		log.Println("ApiServer's answer for getHist request well-received")
		return &result, nil
	}
}

func (s *DATAServer) Price(ctx context.Context, request *pb.PriceDataRequest) (*pb.PriceDataResponse, error) {
	log.Println("Received a Price  request from the ML client!")
	d, _ := os.ReadFile("cache_argument.txt")
	boolean_cache := string(d)
	if boolean_cache == "false" {
		use_serialized_cache = false
	} else {
		use_serialized_cache = true
	}
	if use_serialized_cache {
		if err_cache != nil && bool_cache != true {
			gob.Register(HistDataCache{})
			gob.Register(HistDataCacheLots{})
			gob.Register(StaticDataCache{})
			gob.Register(StaticDataCacheLots{})
			file2.Close()
			file3, _ := os.Open("serialization.gob")
			decoder3 := gob.NewDecoder(file3)
			cc3 := map[string]cache.Item{}
			_ = decoder3.Decode(&cc3)
			c = cache.NewFrom(cache.NoExpiration, 1*time.Minute, cc3)
			file3.Close()
			bool_cache = true
		}
	}
	addr := "localhost:9999"
	conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		log.Fatal("Failed to establish a connection to the api server for Price request", err)
	}
	defer conn.Close()
	client := pbB.NewAPIClient(conn)
	value, found := c.Get("Price/" + request.NameDS)
	if found {
		v := reflect.ValueOf(value).Interface().(HistDataCache)
		end_date, _ := time.Parse("2006-01-02", v.DateCache[0][:10])
		start_date, _ := time.Parse("2006-01-02", v.DateCache[len(v.DateCache)-1][:10])
		start_date_request, _ := time.Parse("2006-01-02", request.StartDateDS)
		end_date_request, _ := time.Parse("2006-01-02", request.EndDateDS)
		if (start_date_request.After(start_date) || start_date_request.Equal(start_date)) && (end_date_request.Before(end_date) || end_date_request.Equal(end_date)) {
			log.Println("The time limits are found in the cache...(case 1)!")
			diff1 := start_date_request.Sub(start_date)
			diff2 := end_date.Sub(end_date_request)
			j1 := int(diff1.Hours() / 24)
			j2 := int(diff2.Hours() / 24)
			result := pb.PriceDataResponse{
				OpenDS:      v.OpenCache[j2 : len(v.DateCache)-j1],
				CloseDS:     v.CloseCache[j2 : len(v.DateCache)-j1],
				HighDS:      v.HighCache[j2 : len(v.DateCache)-j1],
				LowDS:       v.LowCache[j2 : len(v.DateCache)-j1],
				DateDS:      v.DateCache[j2 : len(v.DateCache)-j1],
				VolumeDS:    v.VolumeCache[j2 : len(v.VolumeCache)-j1],
				MarketcapDS: v.MarketCapCache[j2 : len(v.MarketCapCache)-j1],
			}
			return &result, nil

		} else if (start_date_request.Before(start_date) || start_date_request.Equal(start_date)) && (end_date_request.After(end_date) || end_date_request.Equal(end_date)) {
			log.Println("The time limits are not  found in the cache...(case 2) superior to what we have in the cache!")
			request1 := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: v.DateCache[0],
				EndDateHist:    request.EndDateDS,
			}
			request2 := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: request.StartDateDS,
				EndDateHist:    v.DateCache[len(v.DateCache)-1],
			}
			result := pb.PriceDataResponse{}
			result1, _ := client.GetPrice(context.Background(), &request1)
			result2, _ := client.GetPrice(context.Background(), &request2)
			if len(result1.OpenPrice) != 0 {
				var all_openPrice = []float64{}
				all_openPrice = append(all_openPrice, result1.OpenPrice[:len(result1.DatePrice)-1]...)
				all_openPrice = append(all_openPrice, v.OpenCache...)
				all_openPrice = append(all_openPrice, result2.OpenPrice[1:]...)
				var all_closePrice = []float64{}
				all_closePrice = append(all_closePrice, result1.ClosePrice[:len(result1.DatePrice)-1]...)
				all_closePrice = append(all_closePrice, v.CloseCache...)
				all_closePrice = append(all_closePrice, result2.ClosePrice[1:]...)
				var all_highPrice = []float64{}
				all_highPrice = append(all_highPrice, result1.HighPrice[:len(result1.DatePrice)-1]...)
				all_highPrice = append(all_highPrice, v.HighCache...)
				all_highPrice = append(all_highPrice, result2.HighPrice[1:]...)
				var all_lowPrice = []float64{}
				all_lowPrice = append(all_lowPrice, result1.LowPrice[:len(result1.DatePrice)-1]...)
				all_lowPrice = append(all_lowPrice, v.LowCache...)
				all_lowPrice = append(all_lowPrice, result2.LowPrice[1:]...)
				var all_date = []string{}
				all_date = append(all_date, result1.DatePrice[:len(result1.DatePrice)-1]...)
				all_date = append(all_date, v.DateCache...)
				all_date = append(all_date, result2.DatePrice[1:]...)
				var all_volume = []float64{}
				all_volume = append(all_volume, result1.Volume[:len(result1.Volume)-1]...)
				all_volume = append(all_volume, v.VolumeCache...)
				all_volume = append(all_volume, result2.Volume[1:]...)
				var all_marketCap = []float64{}
				all_marketCap = append(all_marketCap, result1.MarketCap[:len(result1.MarketCap)-1]...)
				all_marketCap = append(all_marketCap, v.MarketCapCache...)
				all_marketCap = append(all_marketCap, result2.MarketCap[1:]...)
				to_be_cached := HistDataCache{
					OpenCache:      all_openPrice,
					CloseCache:     all_closePrice,
					HighCache:      all_highPrice,
					LowCache:       all_lowPrice,
					VolumeCache:    all_volume,
					MarketCapCache: all_marketCap,
					DateCache:      all_date,
				}
				result = pb.PriceDataResponse{
					OpenDS:      all_openPrice,
					CloseDS:     all_closePrice,
					HighDS:      all_highPrice,
					LowDS:       all_lowPrice,
					VolumeDS:    all_volume,
					MarketcapDS: all_marketCap,
					DateDS:      all_date,
				}
				c.Set("Price/"+request.NameDS, to_be_cached, cache.NoExpiration)
			} else {
				result = pb.PriceDataResponse{
					OpenDS:      v.OpenCache[:],
					CloseDS:     v.CloseCache[:],
					HighDS:      v.HighCache[:],
					LowDS:       v.LowCache[:],
					DateDS:      v.DateCache[:],
					VolumeDS:    v.VolumeCache[:],
					MarketcapDS: v.MarketCapCache[:],
				}
			}
			return &result, nil

		} else if (start_date_request.After(start_date) || start_date_request.Equal(start_date)) && (end_date_request.After(end_date) || end_date_request.Equal(end_date)) {
			log.Println("The time limits are partially found in the cache...(case 3), the end date given proceeds the end date found in the cache!")
			request3 := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: v.DateCache[0],
				EndDateHist:    request.EndDateDS,
			}
			diff := end_date.Sub(start_date_request)
			j := int(diff.Hours() / 24)
			result, _ := client.GetPrice(context.Background(), &request3)
			final_result := pb.PriceDataResponse{}
			if len(result.OpenPrice) != 0 {
				aux_open := v.OpenCache[:j+1]
				aux_close := v.CloseCache[:j+1]
				aux_high := v.HighCache[:j+1]
				aux_low := v.LowCache[:j+1]
				aux_date := v.DateCache[:j+1]
				aux_volume := v.VolumeCache[:j+1]
				aux_marketcap := v.MarketCapCache[:j+1]
				var all_openPrice = []float64{}
				all_openPrice = append(all_openPrice, result.OpenPrice...)
				all_openPrice = append(all_openPrice, v.OpenCache...)
				var all_closePrice = []float64{}
				all_closePrice = append(all_closePrice, result.ClosePrice...)
				all_closePrice = append(all_closePrice, v.CloseCache...)
				var all_highPrice = []float64{}
				all_highPrice = append(all_highPrice, result.HighPrice...)
				all_highPrice = append(all_highPrice, v.HighCache...)
				var all_lowPrice = []float64{}
				all_lowPrice = append(all_lowPrice, result.LowPrice...)
				all_lowPrice = append(all_lowPrice, v.LowCache...)
				var all_date = []string{}
				all_date = append(all_date, result.DatePrice...)
				all_date = append(all_date, v.DateCache...)
				var all_volume = []float64{}
				all_volume = append(all_volume, result.Volume...)
				all_volume = append(all_volume, v.VolumeCache...)
				var all_marketcap = []float64{}
				all_marketcap = append(all_marketcap, result.MarketCap...)
				all_marketcap = append(all_marketcap, v.MarketCapCache...)
				to_be_cached := HistDataCache{
					OpenCache:      all_openPrice,
					CloseCache:     all_closePrice,
					HighCache:      all_highPrice,
					LowCache:       all_lowPrice,
					VolumeCache:    all_volume,
					MarketCapCache: all_marketcap,
					DateCache:      all_date,
				}
				var final_list_open = []float64{}
				final_list_open = append(final_list_open, result.OpenPrice...)
				final_list_open = append(final_list_open, aux_open...)
				var final_list_close = []float64{}
				final_list_close = append(final_list_close, result.ClosePrice...)
				final_list_close = append(final_list_close, aux_close...)
				var final_list_high = []float64{}
				final_list_high = append(final_list_high, result.HighPrice...)
				final_list_high = append(final_list_high, aux_high...)
				var final_list_low = []float64{}
				final_list_low = append(final_list_low, result.LowPrice...)
				final_list_low = append(final_list_low, aux_low...)
				var final_list_date = []string{}
				final_list_date = append(final_list_date, result.DatePrice...)
				final_list_date = append(final_list_date, aux_date...)
				var final_list_volume = []float64{}
				final_list_volume = append(final_list_volume, result.Volume...)
				final_list_volume = append(final_list_volume, aux_volume...)
				var final_list_marketcap = []float64{}
				final_list_marketcap = append(final_list_marketcap, result.MarketCap...)
				final_list_marketcap = append(final_list_marketcap, aux_marketcap...)
				c.Set("Price/"+request.NameDS, to_be_cached, cache.NoExpiration)
				final_result = pb.PriceDataResponse{
					OpenDS:      final_list_open,
					CloseDS:     final_list_close,
					HighDS:      final_list_high,
					LowDS:       final_list_low,
					VolumeDS:    final_list_volume,
					MarketcapDS: final_list_marketcap,
					DateDS:      final_list_date,
				}
			} else {
				final_result = pb.PriceDataResponse{
					OpenDS:      v.OpenCache[:],
					CloseDS:     v.CloseCache[:],
					HighDS:      v.HighCache[:],
					LowDS:       v.LowCache[:],
					DateDS:      v.DateCache[:],
					VolumeDS:    v.VolumeCache[:],
					MarketcapDS: v.MarketCapCache[:],
				}
			}
			return &final_result, nil

		} else if start_date_request.After(end_date) || start_date_request.Equal(end_date) {

			log.Println("no data found in the cache for this dates ...(case4)")
			req := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: request.StartDateDS,
				EndDateHist:    request.EndDateDS,
			}
			req2 := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: v.DateCache[0],
				EndDateHist:    req.StartDateGHist,
			}
			res, err := client.GetPrice(context.Background(), &req)
			res2, err := client.GetPrice(context.Background(), &req2)
			if err != nil {
				log.Fatal("Failed to receive data for getPrice request from the api server", err)
			}

			result := pb.PriceDataResponse{
				OpenDS:      res.OpenPrice,
				CloseDS:     res.ClosePrice,
				HighDS:      res.HighPrice,
				LowDS:       res.LowPrice,
				VolumeDS:    res.Volume,
				MarketcapDS: res.MarketCap,
				DateDS:      res.DatePrice,
			}
			var all_open_price = []float64{}
			all_open_price = append(all_open_price, res.OpenPrice...)
			all_open_price = append(all_open_price, res2.OpenPrice...)
			all_open_price = append(all_open_price, v.OpenCache...)
			var all_close_price = []float64{}
			all_close_price = append(all_close_price, res.ClosePrice...)
			all_close_price = append(all_close_price, res2.ClosePrice...)
			all_close_price = append(all_close_price, v.CloseCache...)
			var all_high_price = []float64{}
			all_high_price = append(all_high_price, res.HighPrice...)
			all_high_price = append(all_high_price, res2.HighPrice...)
			all_high_price = append(all_high_price, v.HighCache...)
			var all_low_price = []float64{}
			all_low_price = append(all_low_price, res.LowPrice...)
			all_low_price = append(all_low_price, res2.LowPrice...)
			all_low_price = append(all_low_price, v.LowCache...)
			var all_volumes = []float64{}
			all_volumes = append(all_volumes, res.Volume...)
			all_volumes = append(all_volumes, res2.Volume...)
			all_volumes = append(all_volumes, v.VolumeCache...)
			var all_marketcaps = []float64{}
			all_marketcaps = append(all_marketcaps, res.MarketCap...)
			all_marketcaps = append(all_marketcaps, res2.MarketCap...)
			all_marketcaps = append(all_marketcaps, v.MarketCapCache...)
			var all_dates = []string{}
			all_dates = append(all_dates, res.DatePrice...)
			all_dates = append(all_dates, res2.DatePrice...)
			all_dates = append(all_dates, v.DateCache...)
			to_be_cached := HistDataCache{
				OpenCache:      all_open_price,
				CloseCache:     all_close_price,
				HighCache:      all_high_price,
				LowCache:       all_low_price,
				VolumeCache:    all_volumes,
				MarketCapCache: all_marketcaps,
				DateCache:      all_dates,
			}
			c.Set("Price/"+request.NameDS, to_be_cached, cache.NoExpiration)
			return &result, nil

		} else if end_date_request.Before(start_date) || end_date_request.Equal(start_date) {

			log.Println("no data found in the cache for this dates ...(case5)")
			req := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: request.StartDateDS,
				EndDateHist:    request.EndDateDS,
			}
			req2 := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: request.EndDateDS,
				EndDateHist:    v.DateCache[len(v.DateCache)-1],
			}
			res, _ := client.GetPrice(context.Background(), &req)
			res2, err := client.GetPrice(context.Background(), &req2)
			if err != nil {
				log.Fatal("Failed to receive data for getPrice request from the api server", err)
			}

			result := pb.PriceDataResponse{
				OpenDS:      res.OpenPrice,
				CloseDS:     res.ClosePrice,
				HighDS:      res.HighPrice,
				LowDS:       res.LowPrice,
				VolumeDS:    res.Volume,
				MarketcapDS: res.MarketCap,
				DateDS:      res.DatePrice,
			}
			var all_open_price = []float64{}
			all_open_price = append(all_open_price, v.OpenCache...)
			all_open_price = append(all_open_price, res2.OpenPrice...)
			all_open_price = append(all_open_price, res.OpenPrice...)
			var all_close_price = []float64{}
			all_close_price = append(all_close_price, v.CloseCache...)
			all_close_price = append(all_close_price, res2.ClosePrice...)
			all_close_price = append(all_close_price, res.ClosePrice...)
			var all_high_price = []float64{}
			all_high_price = append(all_high_price, v.HighCache...)
			all_high_price = append(all_high_price, res2.HighPrice...)
			all_high_price = append(all_high_price, res.HighPrice...)
			var all_low_price = []float64{}
			all_low_price = append(all_low_price, v.LowCache...)
			all_low_price = append(all_low_price, res2.LowPrice...)
			all_low_price = append(all_low_price, res.LowPrice...)
			var all_volumes = []float64{}
			all_volumes = append(all_volumes, v.VolumeCache...)
			all_volumes = append(all_volumes, res2.Volume...)
			all_volumes = append(all_volumes, res.Volume...)
			var all_marketcaps = []float64{}
			all_marketcaps = append(all_marketcaps, v.MarketCapCache...)
			all_marketcaps = append(all_marketcaps, res2.MarketCap...)
			all_marketcaps = append(all_marketcaps, res.MarketCap...)
			var all_dates = []string{}
			all_dates = append(all_dates, v.DateCache...)
			all_dates = append(all_dates, res2.DatePrice...)
			all_dates = append(all_dates, res.DatePrice...)
			to_be_cached := HistDataCache{
				OpenCache:      all_open_price,
				CloseCache:     all_close_price,
				HighCache:      all_high_price,
				LowCache:       all_low_price,
				VolumeCache:    all_volumes,
				MarketCapCache: all_marketcaps,
				DateCache:      all_dates,
			}
			c.Set("Price/"+request.NameDS, to_be_cached, cache.NoExpiration)
			return &result, nil

		} else {
			log.Println("The time limits are not found in the cache...(case 6) the start date given precedes the start date in our cache!")
			request3 := pbB.PriceRequest{
				Name:           request.NameDS,
				StartDateGHist: request.StartDateDS,
				EndDateHist:    v.DateCache[len(v.DateCache)-1],
			}
			diff := end_date.Sub(end_date_request)
			j := int(diff.Hours() / 24)
			result, _ := client.GetPrice(context.Background(), &request3)
			aux_open := v.OpenCache[j:]
			aux_close := v.CloseCache[j:]
			aux_high := v.HighCache[j:]
			aux_low := v.LowCache[j:]
			aux_date := v.DateCache[j:]
			aux_volume := v.VolumeCache[j:]
			aux_marketcap := v.MarketCapCache[j:]
			var all_openPrice = []float64{}
			all_openPrice = append(all_openPrice, v.OpenCache...)
			all_openPrice = append(all_openPrice, result.OpenPrice...)
			var all_closePrice = []float64{}
			all_closePrice = append(all_closePrice, v.CloseCache...)
			all_closePrice = append(all_closePrice, result.ClosePrice...)
			var all_highPrice = []float64{}
			all_highPrice = append(all_highPrice, v.HighCache...)
			all_highPrice = append(all_highPrice, result.HighPrice...)
			var all_lowPrice = []float64{}
			all_lowPrice = append(all_lowPrice, v.LowCache...)
			all_lowPrice = append(all_lowPrice, result.LowPrice...)
			var all_date = []string{}
			all_date = append(all_date, v.DateCache...)
			all_date = append(all_date, result.DatePrice...)
			var all_volume = []float64{}
			all_volume = append(all_volume, v.VolumeCache...)
			all_volume = append(all_volume, result.Volume...)
			var all_marketCap = []float64{}
			all_marketCap = append(all_marketCap, v.MarketCapCache...)
			all_marketCap = append(all_marketCap, result.MarketCap...)
			to_be_cached := HistDataCache{
				OpenCache:      all_openPrice,
				CloseCache:     all_closePrice,
				HighCache:      all_highPrice,
				LowCache:       all_lowPrice,
				VolumeCache:    all_volume,
				MarketCapCache: all_marketCap,
				DateCache:      all_date,
			}
			var final_list_open = []float64{}
			final_list_open = append(final_list_open, aux_open...)
			final_list_open = append(final_list_open, result.OpenPrice...)
			var final_list_close = []float64{}
			final_list_close = append(final_list_close, aux_close...)
			final_list_close = append(final_list_close, result.ClosePrice...)
			var final_list_high = []float64{}
			final_list_high = append(final_list_high, aux_high...)
			final_list_high = append(final_list_high, result.HighPrice...)
			var final_list_low = []float64{}
			final_list_low = append(final_list_low, aux_low...)
			final_list_low = append(final_list_low, result.LowPrice...)
			var final_list_date = []string{}
			final_list_date = append(final_list_date, aux_date...)
			final_list_date = append(final_list_date, result.DatePrice...)
			var final_list_volume = []float64{}
			final_list_volume = append(final_list_volume, aux_volume...)
			final_list_volume = append(final_list_volume, result.Volume...)
			var final_list_marketcap = []float64{}
			final_list_marketcap = append(final_list_marketcap, aux_marketcap...)
			final_list_marketcap = append(final_list_marketcap, result.MarketCap...)
			c.Set("Price/"+request.NameDS, to_be_cached, cache.NoExpiration)
			final_result := pb.PriceDataResponse{
				OpenDS:      final_list_open,
				CloseDS:     final_list_close,
				HighDS:      final_list_high,
				LowDS:       final_list_low,
				VolumeDS:    final_list_volume,
				MarketcapDS: final_list_marketcap,
				DateDS:      final_list_date,
			}
			return &final_result, nil

		}
	} else {
		req := pbB.PriceRequest{
			Name:           request.NameDS,
			StartDateGHist: request.StartDateDS,
			EndDateHist:    request.EndDateDS,
		}
		res, err := client.GetPrice(context.Background(), &req)
		if err != nil {
			log.Fatal("Failed to receive data for getPrice request from the api server", err)
		}
		result := pb.PriceDataResponse{
			OpenDS:      res.OpenPrice,
			CloseDS:     res.ClosePrice,
			HighDS:      res.HighPrice,
			LowDS:       res.LowPrice,
			VolumeDS:    res.Volume,
			MarketcapDS: res.MarketCap,
			DateDS:      res.DatePrice,
		}
		to_be_cached := HistDataCache{
			OpenCache:      res.OpenPrice,
			CloseCache:     res.ClosePrice,
			HighCache:      res.HighPrice,
			LowCache:       res.LowPrice,
			VolumeCache:    res.Volume,
			MarketCapCache: res.MarketCap,
			DateCache:      res.DatePrice,
		}
		c.Set("Price/"+req.Name, to_be_cached, cache.NoExpiration)
		log.Println("ApiServer's answer for Price request well-received")
		return &result, nil
	}
}

func (s *DATAServer) Stat(ctx context.Context, request *pb.StatDataRequest) (*pb.StatDataResponse, error) {
	log.Println("Received a Stat  request from the ML client!")
	d, _ := os.ReadFile("cache_argument.txt")
	boolean_cache := string(d)
	if boolean_cache == "false" {
		use_serialized_cache = false
	} else {
		use_serialized_cache = true
	}
	if use_serialized_cache {
		if err_cache != nil && bool_cache != true {
			gob.Register(HistDataCache{})
			gob.Register(HistDataCacheLots{})
			gob.Register(StaticDataCache{})
			gob.Register(StaticDataCacheLots{})
			file2.Close()
			file3, _ := os.Open("serialization.gob")
			decoder3 := gob.NewDecoder(file3)
			cc3 := map[string]cache.Item{}
			_ = decoder3.Decode(&cc3)
			c = cache.NewFrom(cache.NoExpiration, 1*time.Minute, cc3)
			file3.Close()
			bool_cache = true
		}
	}
	value, found := c.Get("Stat/" + request.Name)
	if found {
		var aux_list = []string{}
		switch reflect.TypeOf(value).Kind() {
		case reflect.Struct:
			v := reflect.ValueOf(value)
			for j := 0; j < v.NumField(); j++ {
				str_value := fmt.Sprintf("%v", v.Field(j))
				aux_list = append(aux_list, str_value)
			}
		}
		result := pb.StatDataResponse{
			CirculatingSupplyDS: aux_list[0],
			TotalSupplyDS:       aux_list[1],
			MaxSupplyDS:         aux_list[2],
			DateFirstListingDS:  aux_list[3],
			CommitsDS:           &aux_list[4],
			ForksDS:             &aux_list[5],
			StarsDS:             &aux_list[6],
			WatchingDS:          &aux_list[7],
		}
		log.Println("Found the crypto static data stored in the cache and returned it...")
		return &result, nil
	} else {
		addr := "localhost:9999"
		conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			log.Fatal("Failed to establish a connection to the api server for Price request", err)
		}
		defer conn.Close()
		client := pbB.NewAPIClient(conn)
		req := pbB.StaticDataRequest{
			Name: request.Name,
		}
		res, err := client.GetStaticData(context.Background(), &req)
		if err != nil {
			log.Fatal("Failed to receive data for getStaticData request from the api server", err)
		}
		result := pb.StatDataResponse{
			CirculatingSupplyDS: res.CirculatingSupply,
			TotalSupplyDS:       res.TotalSupply,
			MaxSupplyDS:         res.MaxSupply,
			DateFirstListingDS:  res.DateFirstListing,
			CommitsDS:           res.Commits,
			ForksDS:             res.Forks,
			StarsDS:             res.Stars,
			WatchingDS:          res.Watching,
		}
		static_data_to_cache := StaticDataCache{
			CirculatingSupplyCache: res.CirculatingSupply,
			TotalSupplyCache:       res.TotalSupply,
			MaxSupplyCache:         res.MaxSupply,
			DateFirstListingCache:  res.DateFirstListing,
			CommitsCache:           *res.Commits,
			ForksCache:             *res.Forks,
			StarsCache:             *res.Stars,
			WatchingCache:          *res.Watching,
		}
		c.Set("Stat/"+request.Name, static_data_to_cache, cache.NoExpiration)
		log.Println("ApiServer's answer for STAT request well-received")
		return &result, nil
	}
}

func main() {
	log.Println("Started listening on port 50051")
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Failed to launch server due to ", err)
	}
	file_cache, _ := os.Create("cache_argument.txt")
	if len(os.Args) == 1 {
		n, _ := file_cache.WriteString("true")
		n = n + 1
		file_cache.Close()
	} else {
		if os.Args[1] == "false" {
			n, _ := file_cache.WriteString("false")
			n = n + 1
			file_cache.Close()
		} else {
			n, _ := file_cache.WriteString("true")
			n = n + 1
			file_cache.Close()
		}
	}
	gob.Register(HistDataCache{})
	gob.Register(HistDataCacheLots{})
	gob.Register(StaticDataCache{})
	gob.Register(StaticDataCacheLots{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)

	go func() {
		sig := <-sigs

		m := c.Items()

		file4, _ := os.Open("serialization.gob")
		decoder4 := gob.NewDecoder(file4)
		cc4 := map[string]cache.Item{}
		_ = decoder4.Decode(&cc4)
		aux_cache := cache.NewFrom(cache.NoExpiration, 1*time.Minute, cc4)
		file4.Close()
		if c.ItemCount() < aux_cache.ItemCount() {

		} else {
			file, _ := os.Create("serialization.gob")
			encoder := gob.NewEncoder(file)
			err := encoder.Encode(&m)
			if err != nil {
				panic(err)
			}
			file.Close()
		}

		log.Fatal("Data_service closed due to ", sig)
		done <- true
	}()
	log.Printf("Listening on %s", port)
	server := grpc.NewServer()
	pb.RegisterDATAServer(server, &DATAServer{})
	if err := server.Serve(lis); err != nil {
		log.Fatal("Failed to serve ", err)
	}
	<-done
}
