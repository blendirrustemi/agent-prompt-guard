// Run the browser submission logic against a small DOM fixture, without a browser dependency.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { randomUUID } = require("node:crypto");
const vm = require("node:vm");
const path = require("node:path");

function setup() {
    class Element {
        constructor() {
            this.value = "";
            this.children = [];
            this.handlers = {};
            this.dataset = {};
            this.style = {};
            this.isConnected = true;
            this.scrollHeight = 40;
            const classes = new Set();
            this.classList = {
                add: (name) => classes.add(name),
                contains: (name) => classes.has(name),
                toggle: (name, enabled) => enabled ? classes.add(name) : classes.delete(name),
            };
        }
        append(...children) { this.children.push(...children); }
        setAttribute() {}
        removeAttribute() {}
        focus() {}
        getBoundingClientRect() { return { left: 0, top: 0 }; }
        addEventListener(name, callback) { this.handlers[name] = callback; }
    }
    const selectors = ["#prompt-form", "#prompt", "#send-button", "#char-count", "#form-error",
        "#messages", "#workspace", ".composer-area", "#intro"];
    const elements = Object.fromEntries(selectors.map((name) => [name, new Element()]));
    elements["#prompt-form"].dataset.maxLength = "8000";
    elements["#prompt-form"].reset = () => { elements["#prompt"].value = ""; };
    const responses = [];
    const requests = [];
    const timers = new Map();
    let timerId = 0;
    const context = {
        document: { querySelector: (name) => elements[name], createElement: () => new Element() },
        crypto: { randomUUID }, AbortController,
        setTimeout: (callback, delay) => { timers.set(++timerId, { callback, delay }); return timerId; },
        clearTimeout: (id) => timers.delete(id),
        fetch: async (url, options = {}) => {
            requests.push({ url, body: options.body && JSON.parse(options.body) });
            const response = responses.shift();
            assert.ok(response, "Unexpected request");
            return { ok: response.code < 400, json: async () => response.body };
        },
    };
    vm.runInNewContext(readFileSync(path.join(__dirname, "../static/app.js"), "utf8"), context);
    return {
        elements, requests, responses,
        type(text) { elements["#prompt"].value = text; elements["#prompt"].handlers.input(); },
        submit() { return elements["#prompt-form"].handlers.submit({ preventDefault() {} }); },
        async poll() {
            const [id, timer] = [...timers].find(([, value]) => value.delay === 2000);
            timers.delete(id);
            await timer.callback();
        },
        message(index = -1) { return elements["#messages"].children.at(index); },
        status(index = -1) { return this.message(index).children[1].children[1].textContent; },
        response(index = -1) {
            const reply = this.message(index).children[2];
            return reply.hidden ? null : reply.children[1].textContent;
        },
        pendingPolls() { return [...timers.values()].filter(({ delay }) => delay === 2000).length; },
    };
}

test("queued prompt survives storage outages and keeps polling until an answer arrives", async () => {
    const app = setup();
    const prompt = { id: randomUUID(), text: "  Hello 🛡️\n", created_at: new Date().toISOString(), decision: "pending" };
    app.type(prompt.text);
    app.responses.push({ code: 202, body: { prompt, status: "queued" } });
    await app.submit();
    assert.equal(app.status(), "Queued");
    assert.equal(app.elements["#prompt"].value, "");
    app.responses.push({ code: 404, body: { error: "Not stored yet" } });
    await app.poll();
    assert.equal(app.status(), "Queued");
    app.responses.push({ code: 503, body: { error: "Storage unavailable" } });
    await app.poll();
    assert.equal(app.status(), "Queued");
    app.responses.push({ code: 200, body: { prompt, status: "stored" } });
    await app.poll();
    assert.equal(app.status(), "Checking");
    assert.equal(app.pendingPolls(), 1);
    const allowed = { ...prompt, decision: "allow" };
    app.responses.push({ code: 200, body: { prompt: allowed, state: "agent_queued", agent: null } });
    await app.poll();
    assert.equal(app.status(), "Waiting for answer");
    const waiting = app.response();
    app.responses.push({ code: 503, body: { error: "Temporary outage" } });
    await app.poll();
    assert.equal(app.response(), waiting);
    app.responses.push({ code: 200, body: { prompt: allowed, state: "agent_running", agent: { status: "running" } } });
    await app.poll();
    assert.equal(app.status(), "Answering");
    app.responses.push({ code: 200, body: { prompt: allowed, state: "completed", agent: { response: "The answer is 7." } } });
    await app.poll();
    assert.equal(app.status(), "Allowed");
    assert.equal(app.response(), "The answer is 7.");
    assert.equal(app.pendingPolls(), 0);
    assert.equal(app.elements["#messages"].children.length, 1);
    assert.equal(app.requests.filter(({ body }) => body).length, 1);
});

test("already completed answers render immediately as literal text", async () => {
    const app = setup();
    const prompt = { id: randomUUID(), text: "Hello", created_at: new Date().toISOString(), decision: "pending" };
    app.type(prompt.text);
    app.responses.push({ code: 202, body: { prompt, status: "queued" } });
    await app.submit();
    const response = '<img src=x onerror="alert(1)">\nA literal answer.';
    app.responses.push({ code: 200, body: { prompt: { ...prompt, decision: "allow" }, state: "completed", agent: { response } } });
    await app.poll();
    assert.equal(app.response(), response);
    assert.equal(app.message().children[2].children[1].innerHTML, undefined);
    assert.equal(app.pendingPolls(), 0);
});

for (const [state, reason, expected, label] of [
    ["review", "score_in_review_band", "uncertain", "Needs review"],
    ["review", "model_input_too_long", "too long", "Needs review"],
    ["review", "classifier_score_timeout", "couldn’t complete its checks", "Needs review"],
    ["blocked", "score_above_block_threshold", "blocked this prompt", "Blocked"],
    ["blocked", "throttle_burst_detected", "Too many requests", "Blocked"],
    ["agent_failed", null, "couldn’t complete this request", "Answer failed"],
]) {
    test(`${state}/${reason} explains the outcome and stops polling`, async () => {
        const app = setup();
        const prompt = { id: randomUUID(), text: "Check this", created_at: new Date().toISOString(), decision: "pending" };
        app.type(prompt.text);
        app.responses.push({ code: 202, body: { prompt, status: "queued" } });
        await app.submit();
        app.responses.push({ code: 200, body: { prompt, state, guard: { reason }, agent: { error: "private internal details" } } });
        await app.poll();
        assert.equal(app.status(), label);
        assert.ok(app.response().includes(expected));
        assert.ok(!app.response().includes("private internal details"));
        assert.equal(app.pendingPolls(), 0);
        assert.equal(app.requests.length, 2);
    });
}

test("answers finishing out of order stay attached to their own prompt", async () => {
    const app = setup();
    const prompts = ["first", "second"].map((text) => ({
        id: randomUUID(), text, created_at: new Date().toISOString(), decision: "pending",
    }));
    for (const prompt of prompts) {
        app.type(prompt.text);
        app.responses.push({ code: 202, body: { prompt, status: "queued" } });
        await app.submit();
    }
    app.responses.push({ code: 200, body: { prompt: prompts[0], state: "checking" } });
    await app.poll();
    app.responses.push({ code: 200, body: { prompt: prompts[1], state: "completed", agent: { response: "Second answer" } } });
    await app.poll();
    assert.equal(app.response(0), null);
    assert.equal(app.response(1), "Second answer");
    app.responses.push({ code: 200, body: { prompt: prompts[0], state: "completed", agent: { response: "First answer" } } });
    await app.poll();
    assert.equal(app.response(0), "First answer");
    assert.equal(app.response(1), "Second answer");
    assert.equal(app.pendingPolls(), 0);
});

test("a removed prompt stops polling without another request", async () => {
    const app = setup();
    const prompt = { id: randomUUID(), text: "Hello", created_at: new Date().toISOString(), decision: "pending" };
    app.type(prompt.text);
    app.responses.push({ code: 202, body: { prompt, status: "queued" } });
    await app.submit();
    app.message().isConnected = false;
    await app.poll();
    assert.equal(app.requests.length, 1);
    assert.equal(app.pendingPolls(), 0);
});

test("failed submission preserves input and ID, while edited text gets a new ID", async () => {
    const app = setup();
    app.type("Keep this text");
    for (let i = 0; i < 2; i++) {
        app.responses.push({ code: 503, body: { error: "Could not confirm delivery" } });
        await app.submit();
        assert.equal(app.elements["#prompt"].value, "Keep this text");
    }
    assert.equal(app.requests[0].body.prompt_id, app.requests[1].body.prompt_id);
    assert.equal(app.elements["#messages"].children.length, 0);
    app.type("Changed text");
    app.responses.push({ code: 503, body: { error: "Could not confirm delivery" } });
    await app.submit();
    assert.notEqual(app.requests[1].body.prompt_id, app.requests[2].body.prompt_id);
});
