CHANGELOG
---------

v0.31.1
```````

- Implement Protocol.__str__ for real consistent hashing + test (#237)

v0.30.0
```````

- Add ability to return default value on get but breaking get's API
- Support an arbitrary collection of keys, not just a list

v0.29
`````

- added TLS support on #211 thanks to @moisesguimaraes!

v0.28
`````

- moved bmemcached.Client to bmemcached.ReplicantClient *but keeps backward compatibility*
- added DistributedClient to distribute keys over servers using consistent hashing
