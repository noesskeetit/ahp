import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const context={window:{}};vm.createContext(context);vm.runInContext(readFileSync(new URL('./aifactory/ahp-shapes.js',import.meta.url),'utf8'),context);
const {sseDecoder,modelMessages}=context.window.AHP_SHAPES;
test('SSE arbitrary network boundaries preserve genuine delta and cursor',()=>{
 const out=[];const decode=sseDecoder(e=>out.push(e));
 const wire='id: 1\nevent: model_delta\ndata: {"cursor":1,"payload":{"content":"Привет"}}\n\nevent: end\r\ndata: {"type":"end"}\r\n\r\n';
 for(const c of wire)decode(c);
 assert.equal(out.length,2);assert.equal(out[0].payload.content,'Привет');assert.equal(out[1].type,'end');
});
test('replay dedup and final replace never present partial as finished',()=>{
 const delta=(n,text)=>({key:'run:delta:g:'+n,kind:'model_delta',payload:{generation_id:'g',delta_sequence:n,content:text}});
 const frames=[delta(1,'hello '),delta(1,'hello '),delta(2,'world')];
 let messages=modelMessages(frames);assert.equal(messages.length,1);assert.equal(messages[0].payload.content,'hello world');assert.equal(messages[0].partial,true);
 frames.push({kind:'model',key:'run:0',payload:{generation_id:'g',content:'hello world'}});
 messages=modelMessages(frames);assert.equal(messages.length,1);assert.equal(messages[0].partial,false);assert.equal(messages[0].payload.content,'hello world');
});

test('native A2A transcript projection displays text while retaining the raw frame',()=>{
 const frame={kind:'model',key:'run:agent:output:receipt',payload:{step_type:'model',content:{text:'Native answer',a2a:{artifactId:'answer',parts:[{kind:'text',text:'Native answer'}]}}}};
 const result=modelMessages([frame]);
 assert.equal(result.length,1);
 assert.equal(result[0].payload.content,'Native answer');
 assert.equal(result[0].partial,false);
 assert.equal(frame.payload.content.a2a.artifactId,'answer');
});
