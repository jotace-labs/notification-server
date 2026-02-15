# Notification Server

Simple notification server implementation using redis and redpanda.

## Features

Store notifications aynchronously and uses cache to retreive frequently accessed data

## How it works

- set up a notification topic using the kafka compatible service redpanda 
- have any service publish on this topic using the specified notification payload
- the server works as a buffer to store data in mongodb
- this app exposes a http endpoint that (not implemented yet)
  - reads all notification for that service
  - reads all notifications non read for that service
  - reads all notifications from a period of time from that service
- the storage is cached using redis (not implemented yet)
- all stages are instrumented with observability tools (not implemented yet)

## Todos

- implement read operations for mongo package
  - read single notification
  - read all notifications from service
- implement redis caching
- implement api layer
- wrap panic calls so it doesnt quit the app
- instrument everything

## Persistence

Using mongo db to store notifications, as well as its status of reading.
Default conn string for local tests:

```text
mongodb://admin:password@localhost:27017
```

## Message schema

Default topic: `notification.events.v1`

`sentAt` field is optional. If not passed, the saved time will be the systems at that moment (utc).

```json
{
    "service" : "payments",
    "message" : "new order generated",
    "sentAt" : "2026-02-04T21:34:32Z"
}
```

### Aggregation used to sort last notifications from X time

```bson
[
{
 $match: {
   service : "payments",
   sentAt : {
     $gte :   ISODate('2026-02-04T21:38:32Z')
   }
 } 
},
 {
   $sort: {
     sentAt: -1
   }
 }
]
```

finding past notifications

```golang
// "10 minutes ago"
tenMinsAgo := time.Now().UTC().Add(-10 * time.Minute)

### Indexes created

```text
sentAt_-1 -> sort by time
sentAt_text -> filters time
service_1_isRead_1_sentAt_-1 -> match by service, not read and sent at
```

## Timestamps

All services must publish records using UTC timezones. This is the core standart to avoid confusion and incoherence betwen services. The timestamps are also stored in UTC.
Universal at the core, Local at the edges.

The edge services should handle the logic to convert the timestamps to its local timezone.

## local compose config

### redpanda

```text
APP_REDPANDABROKERS="localhost:19092"
APP_KAFKACONSUMERGROUP="notification-group"
APP_NOTIFICATIONTOPIC="notification.events.v1"
```

### mongo

```text
mongodb://admin:password@localhost:27017
```

database name: `notifications`
colletion name: `notifications`
