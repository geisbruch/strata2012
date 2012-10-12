var app = require('express')()
  , server = require('http').createServer(app)
  , io = require('socket.io').listen(server)
  , redis = require("redis");


var sockets = {}

app.get('/', function (req, res) {
  res.sendfile(__dirname + '/view/index.html');
});

var hashs

io.of("/hashs").on("connection",function(socket){
    var interval = setInterval(function(){  
        socket.emit("data",hashs)        
    },500)
    
    socket.on("disconect",function(){
        clearInterval(interval)
    })

})

var redisClient = redis.createClient(6379,"127.0.0.1")
redisClient.on("ready",function(){
    setInterval(function(){
        redisClient.hgetall("hashs",function(err,data){
            var count = 0
            var total = 0
            for(var i in data){
                data[i] = parseInt(data[i])
                count += data[i]
                total++
            }
            var threshold = count / total
            var toShow = {}
            for(var i in data){
                if(data[i]>threshold)
                    toShow[i] = data[i]
            }
            hashs = toShow;
        })
    },200)
})
app.use("/view",require("express").static(__dirname+'/view'));
//app.use("/view", app.static(__dirname + '/view'));

server.listen(8080);

