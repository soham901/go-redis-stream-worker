import { serve } from '@hono/node-server'
import { Hono } from 'hono'

const app = new Hono()

type Statuses = {
  [key: string]: {
    status: string,
    result: any,
    timestamp: number,
    completedAt?: number | null
  }
}

const messageStatuses: Statuses = {}


app.post('/produce', async (c) => {
  try {
    const id = Date.now().toString();
    const messageId = "message-" + id;

    messageStatuses[id] = {
      status: 'pending',
      result: null,
      timestamp: Date.now()
    };
    return c.json({ success: true, messageId, id })
  } catch (e) {
    return c.json({ error: e })
  }
})


app.get('/status/:id', (c) => {
  const { id } = c.req.param();

  if (messageStatuses[id]) {
    return c.json(messageStatuses[id]);
  } else {
    return c.json({ error: 'Message not found' }, { status: 404 });
  }
});


// API to update message status (called by the backend consumer)
app.post('/update-status', async (c) => {
  const { id, status, result } = await c.req.json();

  if (messageStatuses[id]) {
    messageStatuses[id] = {
      ...messageStatuses[id],
      status,
      result,
      completedAt: status === 'completed' ? Date.now() : null
    };
    return c.json({ success: true });
  } else {
    return c.json({ error: 'Message not found' }, { status: 404 });
  }
});


serve({
  fetch: app.fetch,
  port: 3000
}, (info) => {
  console.log(`Server is running on http://localhost:${info.port}`)
})
