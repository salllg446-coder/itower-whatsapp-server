// itower-whatsapp-server
// خادم بسيط يدير جلسة واتساب واحدة (WhatsApp Web multi-device) لكل مستخدم
// عبر مكتبة Baileys غير الرسمية، ويربطها بجداول Supabase:
//   whatsapp_sessions        - حالة الجلسة + رمز QR + بيانات اعتماد الجلسة (creds)
//   whatsapp_auth_keys       - مفاتيح تشفير الجلسة (pre-keys/sessions/sender-keys...)
//   whatsapp_message_queue   - طابور الرسائل المطلوب إرسالها
//
// ملاحظة معمارية مهمة: هذا الخادم مصمم للعمل على استضافة "ephemeral" (حاويات
// مجانية تفقد أي ملف محلي عند إعادة التشغيل، مثل SnapDeploy المجاني). لذلك
// بيانات جلسة واتساب بالكامل تُخزَّن في Supabase وليس على قرص الحاوية -
// أي إعادة تشغيل (نوم/استيقاظ، نشر جديد) لا تفقد الجلسة ولا تتطلب مسح QR
// من جديد. نقطة /ping أدناه تتيح لخدمة مجانية خارجية (GitHub Actions
// المجدولة أو cron-job.org) "إيقاظ" الحاوية دورياً لمنع دخولها بالنوم.
//
// تنبيه: Baileys تحاكي بروتوكول واتساب ويب ولا تستخدم الـ Cloud API الرسمي
// من Meta، وبالتالي فيها خطر حظر فعلي على الرقم المرتبط، خصوصاً مع الإرسال
// الجماعي/المتكرر لأرقام لم تتفاعل معك. هذا الخطر على عاتق صاحب الرقم.

import 'dotenv/config';
import http from 'node:http';
import pino from 'pino';
import qrcode from 'qrcode';
import { createClient } from '@supabase/supabase-js';
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  initAuthCreds,
  BufferJSON,
  proto,
} from '@whiskeysockets/baileys';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  QUEUE_POLL_INTERVAL_MS = '5000',
  PING_PORT = '3000',
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('لازم تضبط SUPABASE_URL و SUPABASE_SERVICE_ROLE_KEY في .env');
}

// service_role key يتجاوز RLS، لذلك الخادم فقط من يجب أن يحمله (ليس التطبيق)
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const logger = pino({ level: 'info' });

// جلسات Baileys النشطة حالياً في الذاكرة: user_id -> { sock, connecting }
const sessions = new Map();

async function updateSessionRow(userId, patch) {
  await supabase
    .from('whatsapp_sessions')
    .update({ ...patch, updated_at: new Date().toISOString() })
    .eq('user_id', userId);
}

// ---------------------------------------------------------------------------
// حالة اعتماد الجلسة (auth state) - مخزّنة بالكامل في Supabase بدل القرص
// المحلي، لتبقى الجلسة سليمة عبر إعادات التشغيل على استضافة ephemeral.
// ---------------------------------------------------------------------------
async function useSupabaseAuthState(userId) {
  const { data: row } = await supabase
    .from('whatsapp_sessions')
    .select('session_creds')
    .eq('user_id', userId)
    .single();

  const creds = row?.session_creds
    ? JSON.parse(JSON.stringify(row.session_creds), BufferJSON.reviver)
    : initAuthCreds();

  const saveCreds = async () => {
    await supabase
      .from('whatsapp_sessions')
      .update({
        session_creds: JSON.parse(JSON.stringify(creds, BufferJSON.replacer)),
      })
      .eq('user_id', userId);
  };

  const keys = {
    get: async (type, ids) => {
      const { data } = await supabase
        .from('whatsapp_auth_keys')
        .select('key_id, value')
        .eq('user_id', userId)
        .eq('category', type)
        .in('key_id', ids);

      const result = {};
      for (const r of data ?? []) {
        let value = JSON.parse(JSON.stringify(r.value), BufferJSON.reviver);
        if (type === 'app-state-sync-key' && value) {
          value = proto.Message.AppStateSyncKeyData.fromObject(value);
        }
        result[r.key_id] = value;
      }
      return result;
    },
    set: async (data) => {
      const upserts = [];
      const deleteTargets = [];

      for (const category of Object.keys(data)) {
        for (const id of Object.keys(data[category])) {
          const value = data[category][id];
          if (value) {
            upserts.push({
              user_id: userId,
              category,
              key_id: id,
              value: JSON.parse(JSON.stringify(value, BufferJSON.replacer)),
              updated_at: new Date().toISOString(),
            });
          } else {
            deleteTargets.push({ category, id });
          }
        }
      }

      if (upserts.length) {
        await supabase
          .from('whatsapp_auth_keys')
          .upsert(upserts, { onConflict: 'user_id,category,key_id' });
      }
      for (const d of deleteTargets) {
        await supabase
          .from('whatsapp_auth_keys')
          .delete()
          .eq('user_id', userId)
          .eq('category', d.category)
          .eq('key_id', d.id);
      }
    },
  };

  return { state: { creds, keys }, saveCreds };
}

// يمسح بيانات الجلسة بالكامل من Supabase (عند تسجيل خروج فعلي من واتساب)
async function clearAuthState(userId) {
  await supabase.from('whatsapp_auth_keys').delete().eq('user_id', userId);
  await updateSessionRow(userId, { session_creds: null });
}

// ---------------------------------------------------------------------------
// إدارة جلسة واتساب لمستخدم واحد
// ---------------------------------------------------------------------------
async function startSessionForUser(userId) {
  if (sessions.has(userId)) return; // الجلسة شغّالة أصلاً

  sessions.set(userId, { sock: null, connecting: true });
  logger.info({ userId }, 'بدء جلسة واتساب جديدة');

  const { state, saveCreds } = await useSupabaseAuthState(userId);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    logger: pino({ level: 'silent' }),
  });

  sessions.set(userId, { sock, connecting: true });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;

    if (qr) {
      // نحوّل نص QR إلى صورة base64 (PNG) يعرضها تطبيق Flutter مباشرة
      const qrBase64 = await qrcode.toDataURL(qr);
      await updateSessionRow(userId, {
        status: 'pending_scan',
        qr_base64: qrBase64,
        last_error: null,
      });
    }

    if (connection === 'open') {
      const linkedPhone = sock.user?.id?.split(':')[0] ?? null;
      sessions.set(userId, { sock, connecting: false });
      await updateSessionRow(userId, {
        status: 'connected',
        qr_base64: null,
        linked_phone: linkedPhone,
        last_error: null,
      });
      logger.info({ userId, linkedPhone }, 'تم الاتصال بواتساب بنجاح');
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      sessions.delete(userId);

      if (loggedOut) {
        // المستخدم فصل الجهاز من واتساب نفسه - يحتاج مسح QR من جديد
        await clearAuthState(userId);
        await updateSessionRow(userId, {
          status: 'disconnected',
          qr_base64: null,
          linked_phone: null,
        });
      } else {
        // انقطاع مؤقت (شبكة، أو إعادة تشغيل الحاوية) - بيانات الجلسة محفوظة
        // في Supabase، فإعادة المحاولة تستأنف بدون طلب مسح QR من جديد
        await updateSessionRow(userId, {
          status: 'error',
          last_error: `disconnected: ${statusCode ?? 'unknown'}`,
        });
        setTimeout(() => startSessionForUser(userId), 5000);
      }
    }
  });
}

// عند إقلاع الخادم من جديد (بعد نوم/استيقاظ الحاوية مثلاً)، أعد الاتصال
// تلقائياً بكل جلسة كانت متصلة سابقاً (creds محفوظة في Supabase)
async function resumeExistingConnectedSessions() {
  const { data } = await supabase
    .from('whatsapp_sessions')
    .select('user_id')
    .not('session_creds', 'is', null)
    .in('status', ['connected', 'error']);

  for (const row of data ?? []) {
    startSessionForUser(row.user_id).catch((e) =>
      logger.error({ e, userId: row.user_id }, 'فشل استئناف الجلسة'),
    );
  }
}

// يُستدعى عند طلب المستخدم "ربط واتساب" من التطبيق (status -> pending_scan)
async function pollForLinkRequests() {
  const { data, error } = await supabase
    .from('whatsapp_sessions')
    .select('user_id, status')
    .eq('status', 'pending_scan');

  if (error) {
    logger.error({ error }, 'فشل جلب طلبات الربط');
    return;
  }

  for (const row of data ?? []) {
    if (!sessions.has(row.user_id)) {
      startSessionForUser(row.user_id).catch((e) =>
        logger.error({ e, userId: row.user_id }, 'فشل بدء الجلسة'),
      );
    }
  }
}

// ---------------------------------------------------------------------------
// معالجة طابور الرسائل: لكل جلسة متصلة، أرسل الرسائل المعلّقة ضمن السقف اليومي
// ---------------------------------------------------------------------------
function randomDelayMs() {
  // تأخير عشوائي بين 4 و 15 ثانية بين كل رسالة وأخرى لتقليل خطر الحظر
  return 4000 + Math.floor(Math.random() * 11000);
}

async function resetDailyCounterIfNeeded(session) {
  const today = new Date().toISOString().slice(0, 10);
  if (session.last_reset_date !== today) {
    await supabase
      .from('whatsapp_sessions')
      .update({ sent_today: 0, last_reset_date: today })
      .eq('user_id', session.user_id);
    session.sent_today = 0;
    session.last_reset_date = today;
  }
}

async function processQueueForUser(userId) {
  const active = sessions.get(userId);
  if (!active?.sock || active.connecting) return;

  const { data: sessionRow } = await supabase
    .from('whatsapp_sessions')
    .select('user_id, daily_send_limit, sent_today, last_reset_date')
    .eq('user_id', userId)
    .single();

  if (!sessionRow) return;
  await resetDailyCounterIfNeeded(sessionRow);

  if (sessionRow.sent_today >= sessionRow.daily_send_limit) return; // بلغ السقف اليومي

  const { data: pending } = await supabase
    .from('whatsapp_message_queue')
    .select('*')
    .eq('user_id', userId)
    .eq('status', 'pending')
    .lte('scheduled_at', new Date().toISOString())
    .order('scheduled_at', { ascending: true })
    .limit(1);

  const msg = pending?.[0];
  if (!msg) return;

  try {
    const jid = `${msg.phone}@s.whatsapp.net`;
    await active.sock.sendMessage(jid, { text: msg.message });

    await supabase
      .from('whatsapp_message_queue')
      .update({ status: 'sent', sent_at: new Date().toISOString() })
      .eq('id', msg.id);

    await supabase
      .from('whatsapp_sessions')
      .update({ sent_today: sessionRow.sent_today + 1 })
      .eq('user_id', userId);
  } catch (e) {
    await supabase
      .from('whatsapp_message_queue')
      .update({ status: 'failed', error_message: String(e?.message ?? e) })
      .eq('id', msg.id);
    logger.error({ e, msgId: msg.id }, 'فشل إرسال رسالة');
  }
}

async function processAllQueues() {
  for (const userId of sessions.keys()) {
    await processQueueForUser(userId);
    // تأخير عشوائي بين كل رسالة ترسلها نفس الجلسة والتي تليها
    await new Promise((r) => setTimeout(r, randomDelayMs()));
  }
}

// ---------------------------------------------------------------------------
// خادم HTTP بسيط لغرض واحد فقط: نقطة /ping يستدعيها مراقب خارجي مجدول
// (GitHub Actions cron أو cron-job.org) كل 20-30 دقيقة لمنع دخول الحاوية
// بوضع النوم على منصات مثل SnapDeploy المجانية (idle timeout ~45 دقيقة).
// ---------------------------------------------------------------------------
const httpServer = http.createServer((req, res) => {
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        ok: true,
        activeSessions: sessions.size,
        time: new Date().toISOString(),
      }),
    );
    return;
  }
  res.writeHead(404);
  res.end();
});

httpServer.listen(Number(PING_PORT), () => {
  logger.info({ port: PING_PORT }, 'خادم /ping جاهز لاستقبال نبضات الإبقاء نشطاً');
});

// ---------------------------------------------------------------------------
const pollIntervalMs = Number(QUEUE_POLL_INTERVAL_MS);
resumeExistingConnectedSessions();
setInterval(pollForLinkRequests, pollIntervalMs);
setInterval(processAllQueues, pollIntervalMs);

logger.info('itower-whatsapp-server started');

