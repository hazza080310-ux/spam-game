// Shadow Royale relay: serves index.html and forwards game messages between host and guests over WebSocket.
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const server=http.createServer((req,res)=>{
  if(req.url==='/healthz'){res.end('ok');return}
  fs.readFile(path.join(__dirname,'index.html'),(e,d)=>{
    if(e){res.writeHead(500);res.end('index.html missing');return}
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});res.end(d)})});
const wss=new WebSocketServer({server,path:'/ws',maxPayload:64*1024});
const rooms=new Map(); // roomId -> {host:ws, guests:Map(guestId -> ws)}
const ID=/^[\w-]{3,40}$/;
const tx=(w,o)=>{if(w&&w.readyState===1)w.send(JSON.stringify(o))};
function leave(ws){
  const r=rooms.get(ws.room);if(!r)return;
  if(ws.role==='host'&&r.host===ws){r.guests.forEach(g=>{tx(g,{k:'closed'});g.room=null});rooms.delete(ws.room)}
  else if(ws.role==='guest'&&r.guests.get(ws.gid)===ws){r.guests.delete(ws.gid);tx(r.host,{k:'bye',id:ws.gid})}
  ws.room=null}
wss.on('connection',ws=>{
  ws.alive=true;ws.on('pong',()=>{ws.alive=true});
  ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(_){return}if(!m||typeof m!=='object')return;
    if(m.k==='reg'){if(!ID.test(m.id)||ws.role)return;
      if(rooms.has(m.id)){tx(ws,{k:'taken'});return}
      rooms.set(m.id,{host:ws,guests:new Map()});ws.role='host';ws.room=m.id;tx(ws,{k:'ok'})}
    else if(m.k==='hi'){if(!ID.test(m.id)||ws.role)return;ws.role='guest';ws.gid=m.id;tx(ws,{k:'ok'})}
    else if(m.k==='join'){const r=ws.role==='guest'&&rooms.get(m.room);
      if(!r){tx(ws,{k:'nohost'});return}
      leave(ws);r.guests.set(ws.gid,ws);ws.room=m.room;tx(r.host,{k:'peer',id:ws.gid});tx(ws,{k:'joined'})}
    else if(m.k==='d'){const r=rooms.get(ws.room);if(!r)return;
      if(ws.role==='host')tx(r.guests.get(m.to),{k:'d',m:m.m});
      else tx(r.host,{k:'d',from:ws.gid,m:m.m})}
    else if(m.k==='kick'&&ws.role==='host'){const r=rooms.get(ws.room),g=r&&r.guests.get(m.to);
      if(g){r.guests.delete(m.to);g.room=null;tx(g,{k:'closed'})}}
    else if(m.k==='leave'&&ws.role==='guest')leave(ws)});
  ws.on('close',()=>leave(ws));ws.on('error',()=>{})});
// keep connections alive through Render's proxy and drop dead ones
setInterval(()=>wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()}),25000);
server.listen(process.env.PORT||3000,()=>console.log('Shadow Royale relay listening'));
