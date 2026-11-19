"use strict";
// UI state belongs to the live session object, so logout removes profile drafts.
window.AHP_PROFILE_ADMIN = {
  create({raw, L, paint, plate, unplate}) {
    const row = cells => window.AHP_SHAPES.tableRow(cells);
    const state = () => L.profileAdmin ||= {profile:null,config:null,routes:[],skills:[],personal:[],admin:null,busy:false};
    const value = name => document.querySelector(`[name="${name}"]`)?.value || "";
    const checked = name => !!document.querySelector(`[name="${name}"]`)?.checked;
    const csv = text => [...new Set(text.split(",").map(s=>s.trim()).filter(Boolean))];
    const area = (label,name,text="") => `<label class="field"><span>${esc(label)}</span><textarea name="${name}" rows="5">${esc(text)}</textarea></label>`;
    const check = (label,name,on) => `<label class="switch"><input type="checkbox" name="${name}" ${on ? "checked" : ""}>${esc(label)}</label>`;
    async function call(op,body={},personal=false) {
      const r=await raw(`/u-ahp/v1/${personal ? "profile" : "registry"}/${op}`,{body});
      if(r.status!==200) {
        const message=r.status===409 ? "Настройки изменились в другой вкладке. Обновите данные перед сохранением." : r.json?.message || r.json?.code || `HTTP ${r.status}`;
        throw Object.assign(new Error(message),{status:r.status});
      }
      return r.json;
    }
    async function load(page) {
      const s=state(); if(s.busy) return; s.busy=true;
      try {
        if(page==="admin") {
          try {s.admin=(await call("AdminAccess")).allowed;} catch(e) {s.admin=false;throw e;}
        }
        const [profile,config,routes,skills,personal]=await Promise.all([
          call("GetProfile",{},true),call("GetConfig",{agent_id:"agents-space"}).catch(e=>{if(e.status===404)return {config:{revision:0,data:{}}};throw e;}),
          call("ListModelRoutes"),call(page==="admin"?"AdminListSkills":"ListSkills"),call("ListPersonalSkills",{},true),
        ]);
        Object.assign(s,{profile:profile.profile,config:config.config,routes:routes.routes || [],skills:skills.skills || [],personal:personal.skills || [],loaded:true});
        unplate("profile_admin");
      } catch(e) {plate("profile_admin",e.message,e.status);} finally {s.busy=false;paint.lastPage="";paint();}
    }
    function skillEditor(personal) {
      const s=state(),current=personal?s.personalEdit:s.skillEdit,prefix=personal?"personal":"skill";
      return panel(personal?"Мои навыки":"Навык tenant",`${field("Название",`pa-${prefix}-id`,current?.skill_id || "","writing")}${personal?"":select("Уровень",`pa-${prefix}-level`,[["user","Сотрудник"],["workspace","Проект"],["admin","Администратор"]],current?.level || "user")}${area("Инструкции навыка",`pa-${prefix}-instructions`,current?.bundle?.instructions || "")}<p class="sub">Ревизия: ${current?.revision || 0}</p><div class="actions">${btn("Сохранить навык",`ahp-pa-${prefix}-save`,"primary")}${btn("Новый навык",`ahp-pa-${prefix}-new`,"secondary")}</div>`);
    }
    function skillList(items,personal) {
      if(!items.length) return notice("Навыков пока нет.");
      return table(["Навык","Ревизия",""],items.map(x=>row([esc(x.skill_id),esc(x.revision),btn("Открыть",personal?"ahp-pa-personal-open":"ahp-pa-skill-open","compact",`data-id="${esc(x.skill_id)}"`)])));
    }
    function profileView() {
      const s=state(),p=s.profile?.data || {},tenant=s.config?.data || {},policy=tenant.policy || {};
      const choices=(items,chosen,name)=>items.map(x=>check(x,`${name}:${x}`,chosen?.includes(x))).join("");
      return heading("Мой Dot","Личные настройки применяются со следующего хода",btn("Обновить","ahp-pa-reload","secondary"))+
        (!s.loaded?notice(s.busy?"Загружаем настройки…":"Загрузите настройки профиля."):panel("Профиль",`<p class="sub">Личная ревизия ${s.profile.revision} · AgentConfig tenant ${s.config.revision}</p>${area("Личные инструкции","pa-instructions",p.instructions)}${!policy.allow_personal_instructions?notice("Администратор запретил личные инструкции."):""}<div class="grid cols2">${select("Маршрут модели","pa-route",[["","По умолчанию tenant"],...(policy.allowed_model_routes || []).map(x=>[x,x])],p.model_route || "")}${field("Лимит шагов","pa-max-steps",p.max_steps || tenant.max_steps || 64,"","number")}${select("Computer","pa-computer",[["","По умолчанию tenant"],...(policy.allowed_computer_policies || []).map(x=>[x,{disabled:"Выключен",ephemeral:"Временный",personal:"Личный"}[x]])],p.computer_policy || "")}</div><h3>Навыки tenant</h3>${choices(policy.allowed_skills || [],p.skills || tenant.skills,"pa-skill")}<h3>Личные навыки</h3>${choices(s.personal.map(x=>x.skill_id),p.personal_skills,"pa-personal")}${!policy.allow_personal_skills?notice("Личные навыки пока не разрешены tenant."):""}<div class="actions">${btn("Сохранить профиль","ahp-pa-profile-save","primary")}</div>`)+skillEditor(true)+panel("Библиотека личных навыков",skillList(s.personal,true)));
    }
    function adminView() {
      const s=state(),c=s.config?.data || {},p=c.policy || {},r=s.routeEdit;
      if(s.admin!==true) return heading("Администрирование tenant","Политики, модели и навыки",btn("Проверить доступ","ahp-pa-reload","secondary"))+notice(s.admin===false?"Нет прав администратора tenant. Изменения недоступны.":"Проверяем права администратора…",s.admin===false?"error":"");
      return heading("Администрирование tenant","Настройки закрепляются отдельно для каждого нового хода",btn("Обновить","ahp-pa-reload","secondary"))+
        panel("AgentConfig и ограничения",`<p class="sub">Ревизия ${s.config?.revision || 0}</p>${area("Общие инструкции","pa-tenant-instructions",c.instructions)}<div class="grid cols2">${field("Модель по умолчанию (маршрут)","pa-default-route",c.model_route || "")}${field("Лимит шагов tenant","pa-tenant-max",c.max_steps || 64,"","number")}${field("Разрешённые маршруты через запятую","pa-allowed-routes",(p.allowed_model_routes || []).join(", "))}${field("Разрешённые навыки через запятую","pa-allowed-skills",(p.allowed_skills || []).join(", "))}${field("Навыки по умолчанию через запятую","pa-default-skills",(c.skills || []).join(", "))}${field("Разрешённые инструменты через запятую","pa-tools",(p.allowed_tools || []).join(", "))}${field("Режимы Computer: disabled, ephemeral, personal","pa-computer-modes",(p.allowed_computer_policies || ["disabled"]).join(", "))}</div>${check("Разрешить личные инструкции","pa-allow-instructions",p.allow_personal_instructions)}${check("Разрешить личные навыки","pa-allow-personal",p.allow_personal_skills)}<div class="actions">${btn("Сохранить политику","ahp-pa-config-save","primary")}</div>`)+
        `<div class="grid cols2">${panel("Маршрут модели",`${field("Имя маршрута","pa-route-id",r?.route || "","fast")}${field("Модель Model Gateway","pa-route-model",r?.document?.model || "")}<p class="sub">Ревизия ${r?.revision || 0}</p><div class="actions">${btn("Сохранить маршрут","ahp-pa-route-save","primary")}${btn("Новый","ahp-pa-route-new","secondary")}${r?btn("Удалить","ahp-pa-route-delete","secondary"):""}</div>`)}${panel("Маршруты",s.routes.length?table(["Маршрут","Ревизия",""],s.routes.map(x=>row([esc(x.route),esc(x.revision),btn("Открыть","ahp-pa-route-open","compact",`data-id="${esc(x.route)}"`)]))):notice("Маршрутов пока нет."))}</div>`+
        `<div class="grid cols2">${skillEditor(false)}${panel("Каталог навыков",skillList(s.skills,false))}</div>`;
    }
    async function hook(name,el) {
      if(!String(name).startsWith("ahp-pa-")) return false;
      const s=state(),page=route()[0].split("?")[0];
      try {
        if(name==="ahp-pa-reload") {await load(page);return true;}
        if(name==="ahp-pa-profile-save") {
          const tenant=s.config?.data || {},data={instructions:value("pa-instructions"),model_route:value("pa-route"),max_steps:Number(value("pa-max-steps")),computer_policy:value("pa-computer"),skills:(tenant.policy?.allowed_skills || []).filter(x=>checked(`pa-skill:${x}`)),personal_skills:s.personal.map(x=>x.skill_id).filter(x=>checked(`pa-personal:${x}`))};
          s.profile=(await call("PutProfile",{expected_revision:s.profile?.revision || 0,data},true)).profile;
        } else if(name==="ahp-pa-config-save") {
          const data={instructions:value("pa-tenant-instructions"),model_route:value("pa-default-route"),max_steps:Number(value("pa-tenant-max")),skills:csv(value("pa-default-skills")),policy:{allowed_model_routes:csv(value("pa-allowed-routes")),allowed_skills:csv(value("pa-allowed-skills")),allowed_tools:csv(value("pa-tools")),allowed_computer_policies:csv(value("pa-computer-modes")),allow_personal_instructions:checked("pa-allow-instructions"),allow_personal_skills:checked("pa-allow-personal")}};
          s.config=(await call("PutConfig",{agent_id:"agents-space",expected_revision:s.config?.revision || 0,data})).config;
        } else if(name==="ahp-pa-route-open") {s.routeEdit=(await call("ResolveModelRoute",{route:el.dataset.id})).route;
        } else if(name==="ahp-pa-route-new") {s.routeEdit=null;
        } else if(name==="ahp-pa-route-save") {s.routeEdit=(await call("PutModelRoute",{route:value("pa-route-id"),expected_revision:s.routeEdit?.route===value("pa-route-id")?s.routeEdit.revision:0,document:{...(s.routeEdit?.document || {}),model:value("pa-route-model")}})).route; await load(page);
        } else if(name==="ahp-pa-route-delete") {await call("DeleteModelRoute",{route:s.routeEdit.route,expected_revision:s.routeEdit.revision});s.routeEdit=null;await load(page);
        } else {
          const personal=name.includes("-personal-"),prefix=personal?"personal":"skill",key=personal?"personalEdit":"skillEdit";
          if(name.endsWith("-new")) s[key]=null;
          else if(name.endsWith("-open")) s[key]=(await call(personal?"GetPersonalSkill":"AdminGetSkill",{skill_id:el.dataset.id},personal)).skill;
          else if(name.endsWith("-save")) {const id=value(`pa-${prefix}-id`);s[key]=(await call(personal?"PutPersonalSkill":"PutSkill",{skill_id:id,expected_revision:s[key]?.skill_id===id?s[key].revision:0,level:personal?"user":value("pa-skill-level"),bundle:{instructions:value(`pa-${prefix}-instructions`)}},personal)).skill;await load(page);}
        }
        unplate("profile_admin"); paint.lastPage=""; paint();
      } catch(e) {plate("profile_admin",e.message,e.status);}
      return true;
    }
    function view(page) {
      if(page!=="profile"&&page!=="admin")return undefined;
      const s=state();
      if(!s.busy&&(!s.loaded || (page==="admin"&&s.admin===null))&&s.loadRequested!==page){s.loadRequested=page;queueMicrotask(()=>load(page));}
      return page==="profile"?profileView():adminView();
    }
    return {view,hook,load};
  }
};
