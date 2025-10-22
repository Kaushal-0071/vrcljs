const express = require('express')
const httpProxy = require('http-proxy')
const { createClient :supabaseclient } = require('@supabase/supabase-js')
const app = express()
const cors = require('cors')
const PORT = process.env.PORT || 8000
require('dotenv').config()
const BASE_PATH = process.env.BUCKET_URL
app.use(cors())
const proxy = httpProxy.createProxy()
const supabaseUrl = process.env.SUPABASE_URL
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = supabaseclient(supabaseUrl, supabaseKey)
app.use(async(req, res) => {
    const hostname = req.hostname;
    const subdomain = hostname.split('.')[0];


    const { data, error } = await supabase
  .from('project')
  .select().eq('sub_domain', subdomain)
         
    const resolvesTo = `${BASE_PATH}/${data[0].id}/`
   
    return proxy.web(req, res, { target: resolvesTo, changeOrigin: true })

})

proxy.on('proxyReq', (proxyReq, req, res) => {
    const url = req.url;
    if (url === '/')
        proxyReq.path += 'index.html'

})

app.listen(PORT, () => console.log(`Reverse Proxy Running..${PORT}`))