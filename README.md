# dedaz


## Tips

Delete all from deadletter queue
```
dedaz peek | jq -r  'map(.sequenceNumber) | join(" ")' | xargs dedaz delete
```

Get deadletter count by subject field
```
dedaz peek | jq '[group_by(.subject)[] | {(.[0].subject): length}] | add'
```