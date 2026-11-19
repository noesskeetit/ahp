import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const context={window:{}};
vm.runInNewContext(readFileSync(new URL("./aifactory/ahp-shapes.js",import.meta.url),"utf8"),context);
const shapes=context.window.AHP_SHAPES;

test("native Hindsight facts have stable references and scopes",()=>{
  const got=shapes.memories({facts:[{memory_ref:"opaque-bank.fact-1",scope:"project",text:"Accepted architecture",type:"world"}]});
  assert.equal(got.missing.length,0);
  assert.equal(got.rows[0].id,"opaque-bank.fact-1");
  assert.equal(got.rows[0].kind,"project");
  assert.equal(got.rows[0].factType,"world");
  assert.equal(got.rows[0].state,"indexed");
});
test("queued operation is never rendered as an indexed fact",()=>{
  const got=shapes.memories({id:"operation-1",state:"queued"});
  assert.equal(got.rows,null);
  assert.equal(shapes.outcome(200,got.rows,got.missing).state,"drift");
});
test("empty page differs from a malformed fact or refused request",()=>{
  assert.equal(shapes.outcome(200,shapes.memories({facts:[]}).rows,[]).state,"empty");
  const bad=shapes.memories({facts:[{text:"Missing owner scope and ref"}]});
  assert.equal(shapes.outcome(200,bad.rows,bad.missing).state,"drift");
  assert.equal(shapes.outcome(403,[],[]).state,"unavailable");
});
test("project binding survives a durable turn projection",()=>{
  const got=shapes.turns({turns:[{id:"run",thread_id:"thread",status:"done",project_id:"alpha"}]});
  assert.equal(got.missing.length,0);
  assert.equal(got.rows[0].projectID,"alpha");
});

test("shared project selection uses the canonical ID and remains explicit",()=>{
  const got=shapes.projectMemoryBinding({workspace:{id:"workspace-id",name:"Visible project"},selectedProfile:"shared"});
  assert.equal(got.projectID,"workspace-id");assert.equal(got.profile,"shared");assert.equal(got.locked,false);assert.equal(got.error,"");
  const native=shapes.projectMemoryBinding({workspace:{id:"workspace-id",name:"Visible project"},selectedProfile:""});
  assert.equal(native.projectID,"Visible project");assert.equal(native.profile,"");
  assert.ok(shapes.projectMemoryBinding({workspace:{name:"Name alone"},selectedProfile:"shared"}).error);
});

test("continuing a conversation never changes its memory backend from form defaults",()=>{
  const got=shapes.projectMemoryBinding({current:{thread_id:"thread",project_id:"original",project_memory_profile:"shared"},workspace:{id:"other",name:"Other"},selectedProfile:"native"});
  assert.equal(got.projectID,"original");assert.equal(got.profile,"shared");assert.equal(got.locked,true);
  const old=shapes.projectMemoryBinding({current:{thread_id:"old",project_id:"private-project"},selectedProfile:"shared"});
  assert.equal(old.profile,"");assert.equal(old.projectID,"private-project");assert.equal(old.locked,true);
});

test("durable shared profile is visible after history reload",()=>{
  const got=shapes.turns({turns:[{id:"run",thread_id:"thread",status:"done",project_id:"workspace-id",project_memory_profile:"shared"}]});
  assert.equal(got.missing.length,0);assert.equal(got.rows[0].projectMemoryProfile,"shared");
});
