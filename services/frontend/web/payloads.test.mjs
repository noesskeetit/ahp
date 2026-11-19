import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const context={window:{}};
vm.createContext(context);
vm.runInContext(readFileSync(new URL('./aifactory/ahp-shapes.js',import.meta.url),'utf8'),context);
const {artifactRef,modelMessages}=context.window.AHP_SHAPES;
const sha='a'.repeat(64);
const ref={id:'artifact-one',version:'immutable-version',sha256:sha,size:2*1024*1024};
const envelope={artifact:ref,preview:'Начало большого результата',truncated:true,reader:'read_output_chunk'};
const frame=(kind,content)=>({kind,sequence:7,payload:{step_type:kind,generation_id:'g1',content}});
test('large model and tool output normalize exact immutable artifact identity',()=>{
 for(const kind of ['model','tool']) {
  assert.deepEqual(JSON.parse(JSON.stringify(artifactRef([frame(kind,envelope)]))),{...ref,sequence:7});
 }
 assert.equal(artifactRef([frame('tool',ref)]).id,ref.id);
 assert.equal(artifactRef([frame('model',ref)]),null,'ordinary model JSON is not an artifact envelope');
 assert.equal(artifactRef([frame('tool',{id:ref.id,version:ref.version,sha256:sha})]).size,null,'legacy handle remains compatible');
});
test('malformed or misplaced artifact envelopes never yield a download handle',()=>{
 for(const invalid of [
  {...envelope,reader:'other'}, {...envelope,truncated:false}, {...envelope,preview:42},
  {...envelope,artifact:{...ref,id:{value:'fake'}}}, {...envelope,artifact:{...ref,version:''}},
  {...envelope,artifact:{...ref,sha256:'z'.repeat(64)}}, {...envelope,artifact:{...ref,size:'123'}},
  {...envelope,artifact:{...ref,size:-1}}, {...envelope,artifact:{...ref,size:1.5}},
  {...envelope,artifact:{...ref,size:65*1024*1024}},
 ]) assert.equal(artifactRef([frame('model',invalid)]),null);
 assert.equal(artifactRef([frame('model_delta',envelope)]),null);
 assert.equal(artifactRef([{kind:'model',payload:{step_type:'tool',content:envelope}}]),null);
});
test('artifact final replaces streaming partial with bounded preview',()=>{
 const delta={kind:'model_delta',payload:{generation_id:'g1',delta_sequence:1,content:'частичный текст'}};
 const messages=modelMessages([delta,frame('model',envelope)]);
 assert.equal(messages.length,1);
 assert.equal(messages[0].partial,false);
 assert.equal(messages[0].payload.content,envelope.preview);
 assert.equal(artifactRef([frame('model',envelope)]).sha256,sha);
});
