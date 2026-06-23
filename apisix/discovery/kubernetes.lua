local core = require("apisix.core")
local discovery = require("apisix.discovery.init")
local ngx = ngx
local ipairs = ipairs
local type = type
local tostring = tostring
local pairs = pairs
local table = table
local string = string
local math = math
local os = os
local cjson = require("cjson.safe")
local lrucache = require("resty.lrucache")
local resty_http = require("resty.http")
local timer_at = ngx.timer.at
local log = core.log
local schema = {
    type = "object",
    properties = {
        service = { type = "object" },
        client = { type = "object" },
        filter = { type = "object" },
    },
}

local plugin_name = "kubernetes"

local _M = {
    version = 0.1,
    priority = 0,
    name = plugin_name,
    schema = schema,
}

local function fetch_endpointslice(service_name, namespace, k8s_client)
    local res, err = k8s_client:request({
        method = "GET",
        path = "/apis/discovery.k8s.io/v1/namespaces/" .. namespace .. "/endpointslices",
        query = { labelSelector = "kubernetes.io/service-name=" .. service_name },
    })
    if not res then
        return nil, "failed to fetch EndpointSlice: " .. err
    end
    if res.status ~= 200 then
        return nil, "unexpected status code: " .. res.status
    end
    local body, err = res:read_body()
    if not body then
        return nil, "failed to read response body: " .. err
    end
    local data, err = cjson.decode(body)
    if not data then
        return nil, "failed to decode JSON: " .. err
    end
    return data, nil
end

local function parse_endpoints(endpointslice_list)
    local nodes = {}
    if not endpointslice_list or not endpointslice_list.items then
        return nodes
    end
    for _, eps in ipairs(endpointslice_list.items) do
        if eps.endpoints then
            for _, ep in ipairs(eps.endpoints) do
                -- Filter out endpoints that are not ready
                if ep.conditions == nil or ep.conditions.ready == nil or ep.conditions.ready == true then
                    if ep.addresses then
                        for _, addr in ipairs(ep.addresses) do
                            local node = {
                                host = addr,
                                port = nil, -- port will be filled later
                                weight = 1,
                            }
                            table.insert(nodes, node)
                        end
                    end
                end
            end
        end
    end
    return nodes
end

local function update_upstream(service_name, namespace, nodes)
    local upstream_name = service_name .. "." .. namespace
    local ok, err = discovery.update_upstream(upstream_name, nodes)
    if not ok then
        log.error("failed to update upstream ", upstream_name, ": ", err)
        return false
    end
    return true
end

local function watch_endpointslices(service_name, namespace, k8s_client)
    local function handler(premature, ...)
        if premature then
            return
        end
        local data, err = fetch_endpointslice(service_name, namespace, k8s_client)
        if not data then
            log.error("failed to fetch EndpointSlice: ", err)
            return
        end
        local nodes = parse_endpoints(data)
        update_upstream(service_name, namespace, nodes)
    end
    local ok, err = timer_at(0, handler)
    if not ok then
        log.error("failed to create timer: ", err)
    end
end

function _M.init_worker()
    local config = core.config.local_conf()
    local discovery_config = config.discovery and config.discovery.kubernetes
    if not discovery_config then
        log.warn("Kubernetes discovery not configured")
        return
    end
    local service = discovery_config.service or {}
    local client_config = discovery_config.client or {}
    local filter = discovery_config.filter or {}
    local k8s_client, err = resty_http.new()
    if not k8s_client then
        log.error("failed to create HTTP client: ", err)
        return
    end
    -- Configure client (e.g., SSL, timeout)
    k8s_client:set_timeout(client_config.timeout or 5000)
    if client_config.ssl_verify then
        k8s_client:ssl_handshake(true, nil, false)
    end
    -- For each service, start watching
    for service_name, namespace in pairs(service) do
        watch_endpointslices(service_name, namespace, k8s_client)
    end
end

return _M