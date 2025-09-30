Mongodb wrapper that allows to communicate with database using http requests.

Only thing you change is
```
local config = {
    ApiKey = "dummy-key", -- Can be any value
    AppId = "dummy-app",  -- Can be any value
    BaseUrl = "http://localhost:3000", -- Point to your network layer
    -- All other config stays the same!
}
```