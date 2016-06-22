package main

import (
    "time"
    "strconv"
    "fmt"
    "net/http"
    "appengine"
    "appengine/datastore"
    "appengine/urlfetch"
    "encoding/json"
    "io/ioutil"
    "log"
)

type ServiceType int64
const (
    ServiceTypeSoundCloud   = iota
    ServiceTypeYouTube
)

type ItemRecord struct {
    Service         ServiceType
    RemoteItemId    int64
    PostTime        time.Time
    Name            string
    Link            string
}

/*
 * Datastore
 */

// itemKey returns the root key used for all item entries for this service type
func itemKey(c appengine.Context, service ServiceType) *datastore.Key {
    return datastore.NewKey(c, "Item", "Service " + strconv.Itoa(int(service)), 0, nil)
}

func latestPostTimeForService(service ServiceType, r *http.Request) (time.Time, error) {
    c := appengine.NewContext(r)
    q := datastore.NewQuery("Item").Ancestor(itemKey(c, service)).Order("-PostTime").Limit(1)

    var items []ItemRecord
    if _, err := q.GetAll(c, &items); err != nil {
        log.Printf("error running GetAll: %v", err)
        return *new(time.Time), err
    }

    if len(items) == 0 {
        log.Print("zero items")
        return *new(time.Time), nil
    }

    log.Printf("items: %v", items)
    return items[0].PostTime, nil
}

// Put a new item record into datastore
func saveItem(record *ItemRecord, r *http.Request) (error) {
    c := appengine.NewContext(r)
    stringID := strconv.Itoa(int(record.Service)) + ":" + strconv.Itoa(int(record.RemoteItemId))
    k := datastore.NewKey(c, "Item", stringID, 0, itemKey(c, record.Service))

    _, err := datastore.Put(c, k, record)
    return err
}

func itemsForService(service ServiceType, r *http.Request) ([]ItemRecord, error) {
    c := appengine.NewContext(r)
    q := datastore.NewQuery("Item").Ancestor(itemKey(c, service)).Order("-PostTime")

    var items []ItemRecord
    if _, err := q.GetAll(c, &items); err != nil {
        log.Printf("error running GetAll: %v", err)
        return items, err
    }

    return items, nil
}

/*
 * Pulling Data
 */

 func pullSoundCloudData(w http.ResponseWriter, r *http.Request, since time.Time) error {
     // Hard coded URL to the Nie Wieder Schlafen page
     url := "http://api.soundcloud.com/users/niewiederschlafen/tracks?client_id=c9aa36e047e11b7b6590dd95e171f91e"

     // Pull the JSON data
     c := appengine.NewContext(r)
     client := urlfetch.Client(c)
     resp, err := client.Get(url)
     if err != nil {
         fmt.Fprintf(w, "Error pulling URL: %v", err)
         return err
     }
     defer resp.Body.Close()
     body, _ := ioutil.ReadAll(resp.Body)

     // Parse the root data into an array of maps, each representing one item
     var target []map[string]json.RawMessage
     json.Unmarshal(body, &target)

     // Loop through the item maps
     for _, element := range target {
         // First check the post time to see if we need to skip it
         var createdAt string
         _ = json.Unmarshal(element["created_at"], &createdAt)
         postTime, _ := time.Parse("2006/01/02 15:04:05 +0000", createdAt)

         if postTime.After(since) {
             // This one is new, so create a record and save it
             var item ItemRecord
             item.Service = ServiceTypeSoundCloud
             item.PostTime = postTime
             _ = json.Unmarshal(element["id"], &item.RemoteItemId)
             _ = json.Unmarshal(element["title"], &item.Name)
             _ = json.Unmarshal(element["permalink_url"], &item.Link)

             saveItem(&item, r)
         }
    }

    return nil
 }

/*
 * Request Handling
 */

func init() {
    http.HandleFunc("/update", updateHandler)
    http.HandleFunc("/feed", feedHandler)
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
    since, err := latestPostTimeForService(ServiceTypeSoundCloud, r)
    if err != nil {
        since = *new(time.Time)
    }
    pullSoundCloudData(w, r, since)

    fmt.Fprintf(w, "<html><body>Updated data for SoundCloud, latest post time is %v</body></html>", since)
}

func feedHandler(w http.ResponseWriter, r *http.Request) {
    items, _ := itemsForService(ServiceTypeSoundCloud, r)
    itemsJSON, _ := json.Marshal(items)
    fmt.Fprint(w, string(itemsJSON))
}
