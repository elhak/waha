import {
  Inject,
  Injectable,
  NotFoundException,
  OnModuleInit,
  UnprocessableEntityException,
} from '@nestjs/common';
import {
  AppsService,
  IAppsService,
} from '@waha/apps/app_sdk/services/IAppsService';
import { EngineBootstrap } from '@waha/core/abc/EngineBootstrap';
import { GowsEngineConfigService } from '@waha/core/config/GowsEngineConfigService';
import { WPPEngineConfigService } from '@waha/core/config/WPPEngineConfigService';
import { WebJSEngineConfigService } from '@waha/core/config/WebJSEngineConfigService';
import { WhatsappSessionGoWSCore } from '@waha/core/engines/gows/session.gows.core';
import { WebhookConductor } from '@waha/core/integrations/webhooks/WebhookConductor';
import { MediaStorageFactory } from '@waha/core/media/MediaStorageFactory';
import { DefaultMap } from '@waha/utils/DefaultMap';
import { getPinoLogLevel, LoggerBuilder } from '@waha/utils/logging';
import { promiseTimeout, sleep } from '@waha/utils/promiseTimeout';
import { complete } from '@waha/utils/reactive/complete';
import { SwitchObservable } from '@waha/utils/reactive/SwitchObservable';
import { PinoLogger } from 'nestjs-pino';
import { Observable, retry, share } from 'rxjs';
import { map } from 'rxjs/operators';

import { getNamespace, getSessionNamespace } from '../config';
import { WhatsappConfigService } from '../config.service';
import {
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
} from '../structures/enums.dto';
import {
  ProxyConfig,
  SessionConfig,
  SessionDetailedInfo,
  SessionDTO,
  SessionInfo,
} from '../structures/sessions.dto';
import { WebhookConfig } from '../structures/webhooks.config.dto';
import { populateSessionInfo, SessionManager } from './abc/manager.abc';
import { SessionParams, WhatsappSession } from './abc/session.abc';
import { EngineConfigService } from './config/EngineConfigService';
import { WhatsappSessionNoWebCore } from './engines/noweb/session.noweb.core';
import { WhatsappSessionWPPCore } from './engines/wpp/session.wpp.core';
import { WhatsappSessionWebJSCore } from './engines/webjs/session.webjs.core';
import { getProxyConfig } from './helpers.proxy';
import { MediaManager } from './media/MediaManager';
import { LocalSessionAuthRepository } from './storage/LocalSessionAuthRepository';
import { LocalStoreCore } from './storage/LocalStoreCore';
import { CoreApiKeyRepository } from './storage/CoreApiKeyRepository';

enum DefaultSessionStatus {
  REMOVED = undefined,
  STOPPED = null,
}

@Injectable()
export class SessionManagerCore extends SessionManager implements OnModuleInit {
  SESSION_STOP_TIMEOUT = 3000;

  // session - exists and running (or failed or smth)
  // null - stopped
  // undefined - removed
  private sessions: Map<string, WhatsappSession | DefaultSessionStatus>;
  private sessionConfigs: Map<string, SessionConfig>;
  DEFAULT = 'default';

  protected readonly EngineClass: typeof WhatsappSession;
  protected events2: DefaultMap<string, DefaultMap<WAHAEvents, SwitchObservable<any>>>;
  protected readonly engineBootstrap: EngineBootstrap;

  constructor(
    config: WhatsappConfigService,
    private engineConfigService: EngineConfigService,
    private webjsEngineConfigService: WebJSEngineConfigService,
    private wppEngineConfigService: WPPEngineConfigService,
    gowsConfigService: GowsEngineConfigService,
    log: PinoLogger,
    private mediaStorageFactory: MediaStorageFactory,
    @Inject(AppsService)
    appsService: IAppsService,
  ) {
    super(log, config, gowsConfigService, appsService);
    this.sessions = new Map();
    this.sessionConfigs = new Map();
    const engineName = this.engineConfigService.getDefaultEngineName();
    this.EngineClass = this.getEngine(engineName);
    this.engineBootstrap = this.getEngineBootstrap(engineName);

    this.events2 = new DefaultMap<string, DefaultMap<WAHAEvents, SwitchObservable<any>>>(
      () =>
        new DefaultMap<WAHAEvents, SwitchObservable<any>>(
          (key) =>
            new SwitchObservable((obs$) => {
              return obs$.pipe(retry(), share());
            }),
        ),
    );

    this.store = new LocalStoreCore(getNamespace(), getSessionNamespace());
    this.sessionAuthRepository = new LocalSessionAuthRepository(this.store);
    this.clearStorage().catch((error) => {
      this.log.error({ error }, 'Error while clearing storage');
    });
  }

  protected getEngine(engine: WAHAEngine): typeof WhatsappSession {
    if (engine === WAHAEngine.WEBJS) {
      return WhatsappSessionWebJSCore;
    } else if (engine === WAHAEngine.WPP) {
      return WhatsappSessionWPPCore;
    } else if (engine === WAHAEngine.NOWEB) {
      return WhatsappSessionNoWebCore;
    } else if (engine === WAHAEngine.GOWS) {
      return WhatsappSessionGoWSCore;
    } else {
      throw new NotFoundException(`Unknown whatsapp engine '${engine}'.`);
    }
  }

  async beforeApplicationShutdown(signal?: string) {
    for (const [name, session] of this.sessions) {
      if (session) {
        await this.stop(name, true);
      }
    }
    this.stopEvents();
    await this.engineBootstrap.shutdown();
  }

  async onApplicationBootstrap() {
    this.apiKeyRepository = new CoreApiKeyRepository();
    await this.engineBootstrap.bootstrap();
    this.startPredefinedSessions();
  }

  private async clearStorage() {
    const storage = await this.mediaStorageFactory.build(
      'all',
      this.log.logger.child({ name: 'Storage' }),
    );
    await storage.purge();
  }

  //
  // API Methods
  //
  async exists(name: string): Promise<boolean> {
    return this.sessions.has(name);
  }

  isRunning(name: string): boolean {
    const session = this.sessions.get(name);
    return !!session;
  }

  async upsert(name: string, config?: SessionConfig): Promise<void> {
    if (!this.sessions.has(name)) {
      this.sessions.set(name, DefaultSessionStatus.STOPPED);
    }
    this.sessionConfigs.set(name, config);
  }

  async start(name: string): Promise<SessionDTO> {
    const session = this.sessions.get(name);
    if (session) {
      throw new UnprocessableEntityException(
        `Session '${name}' is already started.`,
      );
    }
    this.log.info({ session: name }, `Starting session...`);
    const logger = this.log.logger.child({ session: name });
    logger.level = getPinoLogLevel(this.sessionConfigs.get(name)?.debug);
    const loggerBuilder: LoggerBuilder = logger;

    const storage = await this.mediaStorageFactory.build(
      name,
      loggerBuilder.child({ name: 'Storage' }),
    );
    await storage.init();
    const mediaManager = new MediaManager(
      storage,
      this.config.mimetypes,
      loggerBuilder.child({ name: 'MediaManager' }),
    );

    const webhook = new WebhookConductor(loggerBuilder);
    const proxyConfig = this.getProxyConfig(name);
    const sessionConfig: SessionParams = {
      name,
      mediaManager,
      loggerBuilder,
      printQR: this.engineConfigService.shouldPrintQR,
      sessionStore: this.store,
      proxyConfig: proxyConfig,
      sessionConfig: this.sessionConfigs.get(name),
      ignore: this.ignoreChatsConfig(this.sessionConfigs.get(name)),
    };
    if (this.EngineClass === WhatsappSessionWebJSCore) {
      sessionConfig.engineConfig = this.webjsEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionWPPCore) {
      sessionConfig.engineConfig = this.wppEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionGoWSCore) {
      sessionConfig.engineConfig = this.gowsConfigService.getConfig();
    }
    await this.sessionAuthRepository.init(name);
    // @ts-ignore
    const whatsappSession = new this.EngineClass(sessionConfig);
    this.sessions.set(name, whatsappSession);
    this.updateSession(name);

    // configure webhooks
    const webhooks = this.getWebhooks(name);
    webhook.configure(whatsappSession, webhooks);

    // Apps
    try {
      await this.appsService.beforeSessionStart(whatsappSession, this.store);
    } catch (e) {
      logger.error(`Apps Error: ${e}`);
      whatsappSession.status = WAHASessionStatus.FAILED;
    }

    // start session
    if (whatsappSession.status !== WAHASessionStatus.FAILED) {
      await whatsappSession.start();
      logger.info('Session has been started.');
      // Apps
      await this.appsService.afterSessionStart(whatsappSession, this.store);
    }

    // Apps
    await this.appsService.afterSessionStart(whatsappSession, this.store);

    return {
      name: whatsappSession.name,
      status: whatsappSession.status,
      config: whatsappSession.sessionConfig,
    };
  }

  private updateSession(name: string) {
    const session = this.sessions.get(name) as WhatsappSession;
    if (!session) {
      return;
    }
    const sessionEvents = this.events2.get(name);
    for (const eventName in WAHAEvents) {
      const event = WAHAEvents[eventName];
      const stream$ = session
        .getEventObservable(event)
        .pipe(map(populateSessionInfo(event, session)));
      sessionEvents.get(event).switch(stream$);
    }
  }

  getSessionEvent(session: string, event: WAHAEvents): Observable<any> {
    return this.events2.get(session).get(event);
  }

  async stop(name: string, silent: boolean): Promise<void> {
    if (!this.isRunning(name)) {
      this.log.debug({ session: name }, `Session is not running.`);
      return;
    }

    this.log.info({ session: name }, `Stopping session...`);
    try {
      const session = this.getSession(name);
      await session.stop();
    } catch (err) {
      this.log.warn(`Error while stopping session '${name}'`);
      if (!silent) {
        throw err;
      }
    }
    this.log.info({ session: name }, `Session has been stopped.`);
    this.sessions.set(name, DefaultSessionStatus.STOPPED);
    this.updateSession(name);
    await sleep(this.SESSION_STOP_TIMEOUT);
  }

  async unpair(name: string) {
    const session = this.sessions.get(name);
    if (!session) {
      return;
    }
    const whatsappSession = session as WhatsappSession;

    this.log.info({ session: name }, 'Unpairing the device from account...');
    await whatsappSession.unpair().catch((err) => {
      this.log.warn(`Error while unpairing from device: ${err}`);
    });
    await sleep(1000);
  }

  async logout(name: string): Promise<void> {
    await this.sessionAuthRepository.clean(name);
  }

  async delete(name: string): Promise<void> {
    await this.appsService.removeBySession(this, name);
    this.sessions.delete(name);
    this.sessionConfigs.delete(name);
    // Complete and remove events for this session
    const sessionEvents = this.events2.get(name);
    complete(sessionEvents);
    this.events2.delete(name);
  }

  /**
   * Combine per session and global webhooks
   */
  private getWebhooks(name: string) {
    let webhooks: WebhookConfig[] = [];
    const config = this.sessionConfigs.get(name);
    if (config?.webhooks) {
      webhooks = webhooks.concat(config.webhooks);
    }
    const globalWebhookConfig = this.config.getWebhookConfig();
    if (globalWebhookConfig) {
      webhooks.push(globalWebhookConfig);
    }
    return webhooks;
  }

  /**
   * Get either session's or global proxy if defined
   */
  protected getProxyConfig(name: string): ProxyConfig | undefined {
    const config = this.sessionConfigs.get(name);
    if (config?.proxy) {
      return config.proxy;
    }
    const session = this.sessions.get(name) as WhatsappSession;
    if (!session) {
      return undefined;
    }
    const sessions = { [name]: session };
    return getProxyConfig(this.config, sessions, name);
  }

  getSession(name: string): WhatsappSession {
    const session = this.sessions.get(name);
    if (!session) {
      throw new NotFoundException(
        `We didn't find a session with name '${name}'.\n` +
          `Please start it first by using POST /api/sessions/${name}/start request`,
      );
    }
    return session as WhatsappSession;
  }

  async getSessions(all: boolean): Promise<SessionInfo[]> {
    const result: SessionInfo[] = [];
    for (const [name, session] of this.sessions) {
      if (session === DefaultSessionStatus.REMOVED) {
        continue;
      }
      if (session === DefaultSessionStatus.STOPPED && !all) {
        continue;
      }
      if (session === DefaultSessionStatus.STOPPED && all) {
        result.push({
          name: name,
          status: WAHASessionStatus.STOPPED,
          config: this.sessionConfigs.get(name),
          me: null,
          presence: null,
          timestamps: {
            activity: null,
          },
        });
        continue;
      }
      const whatsappSession = session as WhatsappSession;
      const me = whatsappSession?.getSessionMeInfo();
      result.push({
        name: whatsappSession.name,
        status: whatsappSession.status,
        config: whatsappSession.sessionConfig,
        me: me,
        presence: whatsappSession.presence,
        timestamps: {
          activity: whatsappSession?.getLastActivityTimestamp(),
        },
      });
    }
    return result;
  }

  private async fetchEngineInfo(session: WhatsappSession) {
    // Get engine info
    let engineInfo = {};
    if (session) {
      try {
        engineInfo = await promiseTimeout(1000, session.getEngineInfo());
      } catch (error) {
        this.log.debug(
          { session: session.name, error: `${error}` },
          'Can not get engine info',
        );
      }
    }
    const engine = {
      engine: session?.engine,
      ...engineInfo,
    };
    return engine;
  }

  async getSessionInfo(name: string): Promise<SessionDetailedInfo | null> {
    const session = this.sessions.get(name);
    if (!session) {
      return null;
    }
    if (session === DefaultSessionStatus.STOPPED) {
      return {
        name: name,
        status: WAHASessionStatus.STOPPED,
        config: this.sessionConfigs.get(name),
        me: null,
        presence: null,
        timestamps: {
          activity: null,
        },
        engine: {},
      };
    }
    const whatsappSession = session as WhatsappSession;
    const me = whatsappSession?.getSessionMeInfo();
    const engine = await this.fetchEngineInfo(whatsappSession);
    return {
      name: whatsappSession.name,
      status: whatsappSession.status,
      config: whatsappSession.sessionConfig,
      me: me,
      presence: whatsappSession.presence,
      timestamps: {
        activity: whatsappSession?.getLastActivityTimestamp(),
      },
      engine: engine,
    };
  }

  protected stopEvents() {
    for (const sessionEvents of this.events2.values()) {
      complete(sessionEvents);
    }
  }

  async onModuleInit() {
    await this.init();
  }

  async init() {
    await this.store.init();
    const knex = this.store.getWAHADatabase();
    await this.appsService.migrate(knex);
  }
}
