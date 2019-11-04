import { Injectable } from '@angular/core';
import { DbWrapperService, DBTable, DBSchema } from 'db-wrapper';
import { SqlTransaction, SqlResultSet, DataToSync, SqlError, DataFromServer, ProgressData,
  SyncInfo, SyncResult, TableToSync, DataRecord, DataOperation } from './dantia-sync-cli.models';
import { Observable } from 'rxjs';
import { share} from 'rxjs/operators';

@Injectable()
export class DantiaSyncCliService {
  private db: any;
  private schema: DBSchema;
  private tablesToSync: TableToSync[]  = [];
  private idNameFromTableName = {};
  private sizeMax = 1048576;
  private firstSync: boolean;
  private firstSyncDate = 0;
  private clientData;
  private serverData;
  private syncDate: number;
  private username: string;
  private password: string;
  private keyStr: 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
  public serverUrl: string;
  public syncInfo: SyncInfo;
  public cbEndSync: () => void;
  public callBackProgress: (message: string, percent: number, position: string) => void;
  public progress$: Observable<ProgressData>;
  public data$: Observable<DataRecord>;
  private dataObserver: any;
  private progressObserver: any;
  public syncResult: SyncResult = null;

  constructor(private dbWrp: DbWrapperService) {
    this.db = dbWrp.db;
    this.schema = dbWrp.schema;
    this.syncInfo = {uuid: '', version: '0', lastSyncDate: 0};
    let temp = Observable.create( observer => {
      this.progressObserver = observer;
    });
    temp.subscribe(() => {});
    this.progress$ = temp.pipe(share());
    temp = Observable.create ( observer => {
      this.dataObserver = observer;
    });
    temp.subscribe(() => {});
    this.data$ = temp.pipe(share());
  }

  initSync(theServerUrl: string, userInfo: SyncInfo): Promise<void> {
    this.syncInfo.uuid = userInfo.uuid;
    this.syncInfo.version = userInfo.version;
    this.serverUrl = theServerUrl;
    if (userInfo.appName) {
      this.syncInfo.appName = userInfo.appName;
    }
    this.syncInfo.sizeMax = this.sizeMax;
    if (userInfo.username) {
      this.username = userInfo.username;
    }
    if (userInfo.password) {
      this.password = userInfo.password;
    }
    return this.init();
  }

  isRunning(): boolean {
    if (this.syncResult !== null) {
        return true;
    } else {
        return false;
    }
  }

  isPending(): Promise<boolean> {
    if (this.isRunning()) {
      return new Promise(resolve => {
        setTimeout(() => true, 500);
      });
    } else {
      const pendientes =
        [
          new Promise ( resolve => {
            this._selectSql('select count(*) from delete_elem', undefined, undefined, cont => { resolve(cont[0]); } );
          }),
          new Promise ( resolve => {
            this._selectSql('select count(*) from new_elem', undefined, undefined, cont => { resolve(cont[0]); });
          })
        ];
      return Promise.all(pendientes).then( (pdte: boolean[]) => {
        if (pdte[0] || pdte[1]) { return true; } else { return false; }
      });
    }
  }

  getLastSyncdate(): number {
    return this.syncInfo.lastSyncDate;
  }

  setSyncDate(val: number) {
    this.syncInfo.lastSyncDate = val;
    this._executeSql('UPDATE sync_info SET last_sync = ' + this.syncInfo.lastSyncDate, []);
  }

  setFirstSync() {
    this.firstSync = true;
    this.syncInfo.lastSyncDate = 0;
    this._executeSql('UPDATE sync_info SET last_sync = ' + this.syncInfo.lastSyncDate, []);
  }

  log(message) {
    console.log(message);
  }

  logSql(message) {
    console.log(message);
  }

  error(message) {
    console.error(message);
  }

  syncNow(saveBandwidth?: boolean): Observable<SyncResult> {
    const self = this;
    let modelsToBackup = [];
    if (this.db === null) {
        self.log('You should call the initSync before (db is null)');
        throw new Error ('You should call the initSync before (db is null)');
    }

    return Observable.create ( observer => {
      self.cbEndSync = () => {
        this.callBackProgress(self.syncResult.message, 100, self.syncResult.codeStr);
        const resultado = self.syncResult;
        self.syncResult = null;
        observer.next(resultado);
        observer.complete();
      };

      self.callBackProgress = (message, percent, position) => {
        this.progressObserver.next({message, percent, position});
      };

      if (self.syncResult !== null) {
        observer.next(self.syncResult);
      } else {
          self.syncResult = { codeStr: 'noSync',
            message: 'No Sync yet',
            nbDeleted: 0,
            nbSent: 0,
            nbUpdated: 0,
            localDataUpdated: false,
            syncOK: false,
            serverAnswer: null
          }
          // self.syncResult.models = {pendiente = [], completado = []};
          self.tablesToSync.forEach( table => {
              modelsToBackup.push(table.tableName);
          });

          this.callBackProgress('Getting local data to backup', 0, 'getData');

          self.syncDate = Math.round(new Date().getTime()/1000.0);
          self.firstSyncDate = 0;
          self._syncNowGo(modelsToBackup, this.callBackProgress, saveBandwidth);
      }
    });
  }

  /******* PRIVATE FUNCTIONS  ******************/

  private init(): Promise<void> {
    console.log('init');
    this.schema = this.dbWrp.schema;
    this.db = this.dbWrp.db;
    if (this.schema) {
      this.schema.tables.forEach((table: DBTable) => {
        if (table.sync) {
          this.tablesToSync.push({tableName: table.name, idName: table.sync});
          this.idNameFromTableName[table.name] = table.sync;
        }
      });
    }
    return new Promise( (resolve, reject) => {
      this.db.transaction( (tx: SqlTransaction) => {
        this._executeSql('CREATE TABLE IF NOT EXISTS new_elem (table_name TEXT NOT NULL, id TEXT NOT NULL, ' +
            'change_time TIMESTAMP NOT NULL DEFAULT  (strftime(\'%s\',\'now\')));', [], tx);
        this._executeSql('CREATE INDEX IF NOT EXISTS index_tableName_newElem on new_elem (table_name)');
        this._executeSql('CREATE TABLE IF NOT EXISTS delete_elem (table_name TEXT NOT NULL, id TEXT NOT NULL, ' +
            'change_time TIMESTAMP NOT NULL DEFAULT  (strftime(\'%s\',\'now\')));', [], tx);
        this._executeSql('CREATE INDEX IF NOT EXISTS index_tableName_deleteElem on delete_elem (table_name)');
        this._executeSql('CREATE TABLE IF NOT EXISTS sync_info (last_sync TIMESTAMP);', [], tx);

        // create triggers to automatically fill the new_elem table (this table will contains a pointer to all the modified data)
        this.tablesToSync.forEach(curr => {
          this._executeSql('CREATE TRIGGER IF NOT EXISTS update_' + curr.tableName + '  AFTER UPDATE ON ' + curr.tableName + ' ' +
                    'WHEN (SELECT last_sync FROM sync_info) > 0 ' +
                    'BEGIN INSERT INTO new_elem (table_name, id) VALUES ' +
                    '("' + curr.tableName + '", new.' + curr.idName + '); END;', [], tx);

          this._executeSql('CREATE TRIGGER IF NOT EXISTS insert_' + curr.tableName + '  AFTER INSERT ON ' + curr.tableName + ' ' +
                    'WHEN (SELECT last_sync FROM sync_info) > 0 ' +
                    'BEGIN INSERT INTO new_elem (table_name, id) VALUES ' +
                    '("' + curr.tableName + '", new.' + curr.idName + '); END;', [], tx);

          this._executeSql('CREATE TRIGGER IF NOT EXISTS delete_' + curr.tableName + '  AFTER DELETE ON ' + curr.tableName + ' ' +
                    'BEGIN INSERT INTO delete_elem (table_name, id) VALUES ' +
                    '("' + curr.tableName + '", old.' + curr.idName + '); END;', [], tx);
        });
        this._selectSql('SELECT last_sync FROM sync_info', [], tx, res => {
          if (res.length === 0 || res[0] === 0) { // First sync (or data lost)
            if (res.length === 0) {
              this._executeSql('INSERT OR REPLACE INTO sync_info (last_sync) VALUES (0)', [], tx);
            }
            this.firstSync = true;
            this.syncInfo.lastSyncDate = 0;
          } else {
            this.syncInfo.lastSyncDate = res[0];
            if (this.syncInfo.lastSyncDate === 0) {
                this.firstSync = true;
            }
          }
        });
      }, (ts: SqlTransaction, err: SqlError) => {
        this._errorHandler(ts ,err);
        reject(err);
      }, () => {
        console.log('sync activo.');
        resolve();
      });
    });
  }

  private _syncNowGo(modelsToBck: string[],
                     callBackProgress: (message: string, percent: number, position: string) => void, saveBandwidth: boolean) {
    const self = this;
    self._getDataToBackup(modelsToBck, data => {
        self.clientData = data;
        if (saveBandwidth && self.syncResult.nbSent === 0) {
            self.syncResult.localDataUpdated = false;
            self.syncResult.syncOK = true;
            self.syncResult.codeStr = 'nothingToSend';
            self.syncResult.message = 'No new data to send to the server';
            self.cbEndSync();
            return;
        }

        callBackProgress('Sending ' + self.syncResult.nbSent + ' elements to the server', 20, 'sendData');

        self._sendDataToServer(data, (serverData: DataFromServer) => {
          if (!serverData.data || serverData.result === 'ERROR') {
              self.syncResult.syncOK = false;
              if (serverData.status) {
                  self.syncResult.codeStr = serverData.status.toString();
              } else {
                  self.syncResult.codeStr = 'syncKoServer';
              }

              if (serverData.message) {
                  self.syncResult.message = serverData.message;
              } else {
                  self.syncResult.message = 'Datos obtenidos erroneos.';
              }
              self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
              self.error(JSON.stringify(self.syncResult));
              self.cbEndSync();
          } else {
              callBackProgress('Updating local data', 70, 'updateData');
              if (serverData.models.pendiente.length > 0 && self.firstSyncDate === 0) {
                  self.firstSyncDate = serverData.syncDate;
              }
              if (serverData.data.delete_elem) {
                  self.syncResult.nbDeleted = serverData.data.delete_elem.length;
              }
              if (self.firstSync) {
                  self._updateFirstLocalDb(serverData, (sqlErrs: SqlError[]) => {
                    if (sqlErrs) {
                      self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                      self.syncResult.syncOK = false;
                      self.syncResult.codeStr = 'syncKoData';
                      self.syncResult.message = `errors found (${sqlErrs.length}) `;
                      sqlErrs.forEach(err => { self.syncResult.message += err.message + ' '; });
                      self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                      self.cbEndSync();
                    } else if (serverData.models.pendiente.length === 0)  {
                      self._finishSync(serverData.syncDate, () => {
                        self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                        self.syncResult.syncOK = true;
                        self.syncResult.codeStr = 'syncOk';
                        self.syncResult.message = 'First load synchronized successfully. (' + self.syncResult.nbSent +
                            ' new/modified element saved, ' + self.syncResult.nbUpdated + ' updated and ' +
                            self.syncResult.nbDeleted + ' deleted elements.)';
                        self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                        self.cbEndSync();
                      });
                    } else {
                      self._syncNowGo(serverData.models.pendiente, callBackProgress, saveBandwidth);
                    }

                  }, callBackProgress);
              } else {
                  self._updateLocalDb(serverData, (sqlErrs: SqlError[]) => {
                    if (sqlErrs) {
                      self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                      self.syncResult.syncOK = false;
                      self.syncResult.codeStr = 'syncKoData';
                      self.syncResult.message = `errors found (${sqlErrs.length}) `;
                      sqlErrs.forEach(err => { self.syncResult.message += err.message + ' '; });
                      self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                      self.cbEndSync();
                    } else if (serverData.models.pendiente.length === 0)  {
                      self._finishSync(serverData.syncDate, () => {
                        self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                        self.syncResult.syncOK = true;
                        self.syncResult.codeStr = 'syncOk';
                        self.syncResult.message = 'Data synchronized successfully. (' + self.syncResult.nbSent +
                            ' new/modified element saved, ' + self.syncResult.nbUpdated + ' updated and ' +
                            self.syncResult.nbDeleted + ' deleted elements.)';
                        self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                        self.cbEndSync();
                      });
                    } else {
                      self._syncNowGo(serverData.models.pendiente, callBackProgress, saveBandwidth);
                    }
                  }, callBackProgress);
              }
          }
        });
    });

  }

  private _getDataToBackup(modelsToBck: string[], dataCallBack: (datas: DataToSync) => void ): void {
    let nbData = 0;
    const self = this;
    this.log('_getDataToBackup');
    let dataToSync: DataToSync = {
        info: this.syncInfo,
        data: {},
        delete_elem: {}
    };
    self.db.readTransaction( (tx: SqlTransaction) => {
      let counter = 0;
      let nbTables = modelsToBck.length;
      modelsToBck.forEach((tableName: string) => { // a simple for will not work here because we have an asynchronous call inside
        const currTable: TableToSync = self._getTableToProcess(tableName);
        this._getDataToSavDel(currTable.tableName, currTable.idName, this.firstSync, tx, data => {
          dataToSync.data[tableName] = data;
          nbData += data.length;
          counter++;
          if (counter === nbTables) {
            this.log('Data fetched from the local DB');
            this.syncResult.nbSent += nbData;
            dataCallBack(dataToSync);
          }
        });
      }); // end for each

    }, (err: SqlError) => {
      self.log('TransactionError: _getDataToBackup');
      self._errorHandler(undefined ,err);
    }, () => {
      self.log('TransactionFinish: _getDataToBackup');
    });
  }

  private _finishSync(syncDate: number,  callBack?: () => void): void {
    this.firstSync = false;
    this.db.transaction( (tx: SqlTransaction) => {
      if (this.firstSyncDate !== 0) {
          this.syncInfo.lastSyncDate = this.firstSyncDate;
          this._executeSql(`UPDATE sync_info SET last_sync = ${this.firstSyncDate}`, [], tx);
      } else {
          this.syncInfo.lastSyncDate = syncDate;
          this._executeSql(`UPDATE sync_info SET last_sync = ${syncDate}`, [], tx);
      }
      // Remove only the elem sent to the server (in case new_elem has been added during the sync)
      // We don't do that anymore: this._executeSql('DELETE FROM new_elem', [], tx);

      for (let tableName in this.clientData.data) {
        if (this.clientData.data.hasOwnProperty(tableName)) {
          let idsNewToDelete = [];
          let idsDelToDelete = [];
          let idsString = '';
          let idName =  this.idNameFromTableName[tableName];
          this.clientData.data[tableName].forEach(reg => {
              if (reg.TipoOper === 'U') {
                  idsNewToDelete.push(reg[idName]);
              } else {
                  idsDelToDelete.push(reg.IdOper);
              }
          });
          if (idsNewToDelete.length > 0) {
            idsString = idsNewToDelete.map(x => '?').join(',');
            this._executeSql(`DELETE FROM new_elem WHERE table_name ='${tableName}'
                AND id IN (${idsString})
                AND change_time <= ${this.syncDate}`, idsNewToDelete, tx);
          }

          if (idsDelToDelete.length > 0) {
            idsString = idsDelToDelete.map(x => '?').join(',');
            this._executeSql(`DELETE FROM delete_elem WHERE table_name = '${tableName}'
                AND id IN (${idsString})
                AND change_time <= ${this.syncDate}`, idsDelToDelete, tx);
          }
        }
      }
    }, (err: SqlError) => {
      this._errorHandler(undefined, err);
      this.clientData = null;
      this.serverData = null;
      callBack();
    }, () => {
      this.clientData = null;
      this.serverData = null;
      callBack();
    });
  }

  private _getTableToProcess(tableName: string): TableToSync {
    let result: TableToSync;
    this.tablesToSync.forEach( table => {
        if (table.tableName === tableName) {
            result = table;
        }
    });
    if (!result) {
      this.error(tableName + ' no se encuentra entre las tablas a sincronizar.');
    }
    return result;
  }

  private _getDataToSavDel(tableName: string, idName: string, needAllData: boolean, tx: SqlTransaction,
                           dataCallBack: (data: Object[]) => void ): void {
    const sql = 'select distinct op.TipoOper, op.IdOper , c.* ' +
        'from ( ' +
        'select id IdOper, "U" TipoOper, change_time ' +
        'from new_elem ' +
        'where table_name= ? AND change_time <= ? ' +
        ' union ALL ' +
        'select id IdOper, "D" TipoOper, change_time ' +
        'from delete_elem ' +
        'where table_name= ? AND change_time <= ? ' +
        ' order by change_time) op ' +
        'left join ' + tableName + ' c on c.idsync = op.IdOper ' +
        'where (TipoOper="U" and idsync is not null) or TipoOper="D" ' +
        'order by change_time, TipoOper';

    this._selectSql(sql, [tableName, this.syncDate, tableName, this.syncDate], tx, dataCallBack);
  }

  private _getDataToDelete(tableName: string, tx: SqlTransaction, dataCallBack: (data: any[]) => void ): void {
    const sql = 'select distinct id FROM delete_elem' +
        ' WHERE table_name = ? AND change_time <= ?' +
        ' ORDER BY change_time ';
    this._selectSql(sql, [tableName, this.syncDate], tx, dataCallBack);
  }

  private _detectConflict(tableName: string, idValue: any, tx: SqlTransaction, callBack: (detect: boolean) => void): void  {
    let sql: string;
    const self = this;
    if (!this.firstSync) {
        sql = 'select DISTINCT id FROM new_elem ' +
              ' WHERE table_name = ?  AND id = ? AND change_time > ? ' +
              ' UNION ALL ' +
              'select DISTINCT id FROM delete_elem ' +
              ' WHERE table_name = ? AND id = ? AND change_time > ?';

        self._selectSql(sql, [tableName, idValue, this.syncDate, tableName, idValue, this.syncDate], tx,
          (exists) => {
            if (exists.length) { callBack(true); } else { callBack(false); }
          });
    } else {
      callBack(false);
    }
  }

  private _updateRecord(tableName: string, idName: string, reg: Object, tx: SqlTransaction, callBack?: (sqlErr?: SqlError) => void ) {
    let sql: string;
    const self = this;

    this._detectConflict(tableName, reg[idName], tx, exists => {
        if (!exists) {
            /*ex : UPDATE "tableName" SET colonne 1 = [valeur 1], colonne 2 = [valeur 2]*/
            const attList = this._getAttributesList(reg);
            sql = this._buildUpdateSQL(tableName, reg);
            sql += ' WHERE ' + idName + ' = ? ';
            const attValue = this._getMembersValue(reg, attList);

            self._executeSql(sql, [ ...attValue, reg[idName]], tx, () => {
                sql = 'DELETE FROM new_elem WHERE ' +
                    'table_name = ? AND id = ? AND ' +
                    'change_time = (select MAX(change_time) FROM new_elem  ' +
                    'WHERE table_name = ?  AND id = ?) ';

                self._executeSql(sql, [tableName, reg[idName], tableName, reg[idName] ], tx,
                  () => {
                    self.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Updated});
                    callBack();
                  },
                  (ts, error) => {
                    self._errorHandler(ts, error);
                    callBack(error);
                  });
            });

        } else {  // send conflict to server
          callBack();
          self._sendConflict(tableName, idName, reg, tx);
        }
    });
  }

  private _updateLocalDb(serverData: any, callBack: (sqlErrs?: SqlError[]) => void,
                         callBackProgress: (message: string, percent: number, position: string) => void): void {

    const self = this;
    self.serverData = serverData;
    let sqlErrs: SqlError[] = [];
    if (typeof serverData.data === 'undefined' || serverData.data.length === 0) {
        // nothing to update
        callBack();
        return;
    }
    let counterNbTable = 0;
    const nbTables = serverData.models.completado.length;
    const nbTablesPdte = serverData.models.pendiente.length;
    let counterNbElm = 0;
    serverData.models.completado.forEach(tableName => {
      const table = self._getTableToProcess(tableName);
      let currData = serverData.data[table.tableName];
      let deleData = serverData.data.delete_elem[table.tableName];

      if (!currData) {
          // Should always be defined (even if 0 elements)
          // Must not be null
          currData = [];
      }
      if (!deleData) {  deleData = [];   }
      const nb = currData.length;
      const nbDel = deleData.length;
      counterNbElm += nb;
      self.log(`There are ${nb} new or modified elements and ${nbDel} deleted, in the table ${table.tableName}to save in the local DB`);
      let counterNbElmTab = 0;

      self.db.transaction((tx: SqlTransaction) => {
        self._deleteTableLocalDb (table.tableName, table.idName, deleData, tx,  () => {
          let listIdToCheck = [];
          if (nb !== 0) {
            for (let i = 0; i < nb; i++) {
                listIdToCheck.push(currData[i][table.idName]);
            }
            self._getIdExitingInDB(table.tableName, table.idName, listIdToCheck, tx, idInDb => {
              let curr;
              for (let i = 0; i < nb; i++) {
                curr = currData[i];

                if (idInDb[curr[table.idName]]) {// update
                    self._updateRecord(table.tableName, table.idName, curr, tx, (err) => {
                      counterNbElmTab++;
                      if (err) { sqlErrs.push(err); }
                    });
                } else {// insert
                  self._insertRecord(table.tableName, table.idName, curr, tx, (err) => {
                      counterNbElmTab++;
                      if (err) { sqlErrs.push(err); }
                  });
                }
              } // end for
            }); // end getExisting Id
          }
        }); // end delete elements
      }, (err) => {
        counterNbTable++;
        sqlErrs.push(err);
        self.log(`TransactionError: ${err.message}`);
        self._errorHandler(undefined, err);
        if (counterNbTable === nbTables) {  callBack(sqlErrs);   }
      }, () => {
        counterNbTable++;
        const perProgress = this._progressRatio(counterNbTable, nbTables, nbTablesPdte);
        this.syncResult.nbUpdated += counterNbElmTab;
        this.log(`TransactionFinish: ${table.tableName} percent: ${perProgress.toString()}`);
        callBackProgress(table.tableName, perProgress, 'updateLocalDB');
        if (counterNbTable === nbTables) {
          if (sqlErrs.length === 0) { callBack(); }
          else { callBack(sqlErrs); }
        }
      }); // end tx
    }); // end forEach
  }

  private _updateFirstLocalDb(serverData: any, callBack: (sqlErrs?: SqlError[]) => void,
                              callBackProgress: (message: string, percent: number, position: string) => void): void {
    this.serverData = serverData;
    let sqlErrs: SqlError[] = [];

    if (typeof serverData.data === 'undefined' || serverData.data.length === 0) {
        // nothing to update
        // We only use the server date to avoid dealing with wrong date from the client
        callBack();
        return;
    }
    let counterNbTable = 0;
    const nbTables = serverData.models.completado.length;
    const nbTablesPdte = serverData.models.pendiente.length;
    let counterNbElm = 0;

    serverData.models.completado.forEach(tableName => {
        const table = this._getTableToProcess(tableName);
        let currData = serverData.data[table.tableName];
        if (!currData) {
            //Should always be defined (even if 0 elements)
            //Must not be null
            currData = [];
        }
        const nb = currData.length;
        counterNbElm += nb;
        this.log('There are ' + nb + ' new elements, in the table ' + table.tableName + ' to save in the local DB');

        let counterNbElmTab = 0;
        this.db.transaction ( (tx: SqlTransaction) => {
          if (nb !== 0) {
            for (let i = 0; i < nb; i++) {
              this._insertRecord(table.tableName, table.idName, currData[i], tx, (err) => {
                counterNbElmTab++;
                if (err) { sqlErrs.push(err); }

              });
            }
          }
        }, (err) => {
          counterNbTable++;
          sqlErrs.push(err);
          this.log(`TransactionError: ${err.message}`);
          this._errorHandler(undefined, err);
          if (counterNbTable === nbTables) {  callBack(sqlErrs);   }
        }, () => {
          counterNbTable++;
          const perProgress = this._progressRatio(counterNbTable, nbTables, nbTablesPdte);
          this.syncResult.nbUpdated += counterNbElmTab;

          this.log(`TransactionFinish: ${table.tableName}  percent: ${perProgress.toString()}`);
          callBackProgress(table.tableName, perProgress, 'updateFirstLocalDb');
          if (counterNbTable === nbTables) {
            if (sqlErrs.length === 0) { callBack(); }
            else { callBack(sqlErrs); }
          }
        });
    }); // end forEach
  }

  private _insertRecord(tableName: string, idName: string, reg: Object, tx: SqlTransaction, callBack?: (sqlErr?: SqlError) => void) {
    let sql: string;
    const self = this;

    this._detectConflict(tableName, reg[idName], tx,
      (exists) => {
        if (!exists) {

            // 'ex INSERT INTO tablename (id, name, type, etc) VALUES (?, ?, ?, ?);'
            const attList = self._getAttributesList(reg);
            sql = self._buildInsertSQL(tableName, reg);
            const attValue = self._getMembersValue(reg, attList);
            if (!self.firstSync) {
                self._executeSql(sql, attValue, tx, () => {
                    sql = 'DELETE FROM new_elem WHERE ' +
                        'table_name = ? AND id = ? AND ' +
                        'change_time = (select MAX(change_time) FROM new_elem WHERE ' +
                        'table_name = ? AND id = ?) ';

                    self._executeSql(sql, [tableName, reg[idName], tableName, reg[idName]], tx,
                      () => {
                        this.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Inserted});
                        callBack ();
                      });
                }, (ts, error) => {
                  self._errorHandler(ts, error);
                  callBack(error);
                });
            } else {
                self._executeSql(sql, attValue, tx,
                  () => {
                    this.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Inserted});
                    callBack ();
                  },
                  (ts, error) => {
                    self._errorHandler(ts, error);
                    callBack(error);
                });
            }
        } else {  // send conflict to server
            self._sendConflict(tableName, idName, reg, tx);
        }
    });
  }

  private _sendConflict(tableName: string, idName: any, reg: Object, tx: SqlTransaction): void {
    const self = this;
    let sql: string ;

    sql = 'select * FROM ' + tableName + ' WHERE ' + idName + ' = ?';
    self._selectSql(sql, [reg[idName]], tx, regloc => {
        const dataToSend = {
          info: self.syncInfo,
          client: null,
          server: null
        };
        if (regloc.length === 0) {
            dataToSend.client = 'DELETED';
        } else {
            dataToSend.client = regloc;
        }
        dataToSend.server = reg;
        self._sendConflictToServer(dataToSend);
    });
  }

  private _transformRs(rs: SqlResultSet): Object[] {
    let elms = [];
    if (typeof rs.rows === 'undefined') {
        return elms;
    }

    for (let i = 0, ObjKeys; i < rs.rows.length; ++i) {
        ObjKeys = Object.keys(rs.rows.item(i));
        if (ObjKeys.length === 1 ) {
            elms.push(rs.rows.item(i)[ ObjKeys[0] ] );
        } else {
            elms.push(rs.rows.item(i));
        }
    }
    return elms;
  }

  private _deleteTableLocalDb(tablename: string, idName: string, listIdToDelete: any[],
                              tx: SqlTransaction, callBack: (final: boolean) => void): void {
    const self = this;
    if (listIdToDelete.length === 0) {
      callBack(true);
    } else {
      let  sql = `delete from ${tablename} WHERE ${idName} IN (${listIdToDelete.map(x => '?').join(',')})`;
      this._executeSql(sql, listIdToDelete, tx, () => {
        sql = `delete from delete_elem WHERE table_name = "${tablename}" and id  IN (${listIdToDelete.map(x => '?').join(',')})`;
        self._executeSql(sql, listIdToDelete, tx, () => {
          const reg = {};
          listIdToDelete.forEach( x => {
            reg[idName] = x;
            self.dataObserver.next({table: tablename, record: reg, operation: DataOperation.Deleted});
          });
          callBack(true);
        });
      });
    }
  }

  private _getIdExitingInDB(tableName: string, idName: string, listIdToCheck: any[], tx: SqlTransaction,
                            dataCallBack: (IdExiting: boolean[]) => void): void {
    if (listIdToCheck.length === 0) {
      dataCallBack([]);
    } else {
      const sql = `select ${idName} FROM ${tableName} WHERE ${idName} IN ( ${listIdToCheck.map(x => '?').join(',')} )`;
      this._selectSql(sql, listIdToCheck, tx, (idsFind) =>  {
          const idsInDb = [];
          idsFind.forEach(id => { idsInDb[id] = true; } );
          dataCallBack(idsInDb);
      });
    }
  }
/*
  private _batch (execSql:SqlStatement[]): Promise<SqlResultSet[]> {
    let result: SqlResultSet[] = [];
    return new Promise ((resolve, reject) => {
      this.db.transaction ( tx => {
        execSql.some((stat:SqlStatement) => {
          return this._executeSql(stat.sql, stat.params || [],tx).then ( rs => {
            result.push(rs);
            if (stat.successCallback) {
              stat.successCallback(rs);
            }
            return false;
          }, (error) => {
            if (stat.errorCallback) {
              stat.errorCallback(error);
            }
            return true;
          });
        });
      }, error => {
        reject(error);
      }, () => {
        resolve(result);
      });
    });
  }

  private _batchSql  (execSql:SqlStatement[]): Promise<any[]> {
    let result: any[] = [];
    return new Promise ((resolve, reject) => {
      this.db.readTransaction ( tx => {
        execSql.some((stat) => {
          this._selectSql(stat.sql, stat.params || [], tx).then( (rs) => {
            result.push(rs);
            if (stat.successCallback) {
              stat.successCallback(rs);
            }
          }, (ts, error) => {
            if (stat.errorCallback) {
              stat.errorCallback(error);
            }
            return true;
          });
        });
      }, error => {
        reject(error);
      }, () => {
        resolve(result);
      });
    });
  }
*/
  private _executeSqlBridge(tx: SqlTransaction, sql: string, params: any[],
                            dataHandler: (transaction: SqlTransaction, results: SqlResultSet) => void,
                            errorHandler: (transaction: SqlTransaction, error: SqlError) => void) {
    // Standard WebSQL
    tx.executeSql(sql, params, dataHandler,
      (transaction: SqlTransaction, error: SqlError) => {
            this.log('sql error: ' + sql);
            errorHandler(transaction, error);
        }
    );
  }

  private _executeSql(sql: string, params?: any[], optionalTransaction?: SqlTransaction,
                      optionalCallBack?: (transaction: SqlTransaction, results: SqlResultSet) => void,
                      optionalErrorHandler?: (transaction: SqlTransaction, error: SqlError) => void): void {

    const self = this;
    if (params) {
      this.logSql(`_executeSql: ${sql} with param ${params.join(',')}`);
    } else {
      this.logSql(`_executeSql: ${sql}`);
    }
    if (!optionalCallBack) {
        optionalCallBack = self._defaultCallBack;
    }
    if (!optionalErrorHandler) {
        optionalErrorHandler = self._errorHandler;
    }
    if (optionalTransaction) {
        self._executeSqlBridge(optionalTransaction, sql, params || [], optionalCallBack, optionalErrorHandler);
    } else {
      if (sql.indexOf('select') === 0) {
        self.db.readTransaction ((tx: SqlTransaction) => {
            self._executeSqlBridge(tx, sql, params, optionalCallBack, optionalErrorHandler);
        });
      } else {
        self.db.transaction ((tx: SqlTransaction) => {
          self._executeSqlBridge(tx, sql, params, optionalCallBack, optionalErrorHandler);
      });
      }
    }
  }


  private _defaultCallBack(transaction: SqlTransaction, results: SqlResultSet) {
  //DBSYNC.log('SQL Query executed. insertId: '+results.insertId+' rows.length '+results.rows.length);
  }

  private _selectSql(sql: string, params?: any[], optionalTransaction?: SqlTransaction,
                     callBack?: (result: Object[] | any[]) => void): void {
    const self = this;
    this._executeSql(sql, params, optionalTransaction,
      (tx, rs) => { callBack(self._transformRs(rs)); });
  }

  private _errorHandler(transaction: SqlTransaction, error: SqlError) {
    this.log(error);
    this.error('Error : ' + error.message + ' (Code ' + error.code + ')' );
  }

  private _buildInsertSQL(tableName: string, objToInsert: Object): string {
    const members = this._getAttributesList(objToInsert);
    if (members.length === 0) {
        throw new Error('buildInsertSQL : Error, try to insert an empty object in the table ' + tableName);
    }
    // build INSERT INTO myTable (attName1, attName2) VALUES (?, ?) -> need to pass the values in parameters
    let sql = 'INSERT INTO ' + tableName + ' (';
    sql += members.join(',');
    sql += ') VALUES (';
    sql += members.map(x => '?').join(',');
    sql += ')';
    return sql;
  }

  private _buildUpdateSQL(tableName: string, objToUpdate: Object): string {
  /*ex UPDATE "nom de table" SET colonne 1 = [valeur 1], colonne 2 = [valeur 2] WHERE {condition}*/
    let sql = 'UPDATE ' + tableName + ' SET ';
    const members = this._getAttributesList(objToUpdate);
    if (members.length === 0) {
        throw new Error('buildUpdateSQL : Error, try to insert an empty object in the table ' + tableName);
    }
    // let values = this._getMembersValue(objToUpdate, members);

    const nb = members.length;
    for (let i = 0; i < nb; i++) {
        sql += '"' + members[i] + '" = ?';
        if (i < nb - 1) {
            sql += ', ';
        }
    }

    return sql;
  }

  private _replaceAll(value: any, search: string, replacement: string): string {
    if (typeof value === 'string') {
        return value.split(search).join(replacement);
    } else {
        return value;
    }
  }


  private _getMembersValue(obj: Object, members: string[]): any[] {
    const memberArray = [];
    members.forEach( member => {
      memberArray.push(obj[member]);
    });
    return memberArray;
  }

  private _getAttributesList(obj: Object, check?: any): any[] {
    const memberArray = [];
    for (const elm in obj) {
        if (check && typeof this[elm] === 'function' && !obj.hasOwnProperty(elm)) {
            continue;
        }
        memberArray.push(elm);
    }
    return memberArray;
  }

  private _getMembersValueString(obj: Object, members: string[], separator: string): string {
    let result = '';
    for (let i = 0; i < members.length; i++) {
        result += '"' + obj[members[i]] + '"';
        if (i < members.length - 1) {
            result += separator;
        }
    }
    return result;
  }

  private _sendDataToServer(dataToSync: DataToSync, callBack: (serverResponse: DataFromServer) => void): void {
    const self = this;

    let XHR = new XMLHttpRequest();
    let data = JSON.stringify(dataToSync);
    XHR.overrideMimeType('application/json;charset=UTF-8');

    if (self.username !== null && self.password !== null &&
        self.username !== undefined && self.password !== undefined ) {
        XHR.open('POST', self.serverUrl, true);
        XHR.setRequestHeader('Authorization', 'Basic ' + self._encodeBase64(self.username + ':' + self.password));
    } else {
        XHR.open('POST', self.serverUrl, true);
    }

    XHR.setRequestHeader('Content-type', 'application/json; charset=utf-8');
    XHR.onreadystatechange = () => {
        let serverAnswer;
        if (4 === XHR.readyState) {
            if (XHR.status === 0 && XHR.response === '') {
                callBack({ result: 'ERROR',
                    message: 'Se ha producido un error de red.',
                    status: XHR.status,
                    sizeResponse: 0,
                    syncDate: 0,
                    data: {},
                    models: {},
                    responseText : XHR.response});
            }
            try {
                serverAnswer = JSON.parse(XHR.responseText);
            } catch(e) {
                serverAnswer = XHR.responseText;
            }
            self.log('Server answered: ');
            self.log(JSON.stringify({result:serverAnswer.result, message:serverAnswer.message}));
            // I want only json/object as response
            if ((XHR.status === 200) && serverAnswer instanceof Object) {
                callBack(serverAnswer);
            } else if (XHR.status === 500) {
                serverAnswer = {
                    result : 'ERROR',
                    message : 'Se ha producido un error en el servidor.',
                    status : XHR.status,
                    responseText : serverAnswer
                };
                callBack(serverAnswer);
            } else if (XHR.status >= 900 && XHR.status <= 999) {
                callBack(serverAnswer);
            }
        }
    };

    XHR.ontimeout = () => {
        let serverAnswer = {
            result : 'ERROR',
            message : 'Server Time Out',
            status : XHR.status,
            responseText : XHR.responseText,
            data: {},
            models: {},
            sizeResponse: 0,
            syncDate: 0
        };
        callBack(serverAnswer);
    };

    XHR.send(data);

  }

  private _sendConflictToServer(dataConflic) {
    const self = this;

    const XHR = new XMLHttpRequest();
    const data = JSON.stringify(dataConflic);
    XHR.overrideMimeType('application/json;charset=UTF-8');
    XHR.timeout = 60000;
    XHR.setRequestHeader('Content-type', 'application/json; charset=utf-8');

    if (self.username !== null && self.password !== null && self.username !== undefined && self.password !== undefined ){
      XHR.open('POST', self.serverUrl.replace('sync', 'conflict'), true);
      XHR.setRequestHeader('Authorization', 'Basic ' + self._encodeBase64(self.username + ':' + self.password));
    } else {
      XHR.open('POST', self.serverUrl.replace('sync', 'conflict'), true);
    }

    XHR.onreadystatechange =  () => {
        let serverAnswer;
        if (4 === XHR.readyState) {
            if (XHR.status === 0 && XHR.response === '') {
                self.log('Error de red.');
            }
            try {
                serverAnswer = JSON.parse(XHR.responseText);
            } catch (e) {
                serverAnswer = XHR.responseText;
            }
            // I want only json/object as response
            if (XHR.status === 200 && serverAnswer instanceof Object) {
                self.log('Server conflict answered: ');
                self.log(JSON.stringify(serverAnswer));
            } else {
                self.error('Server conflict error answered: ' + JSON.stringify(serverAnswer) );
            }
        }
    };

    XHR.ontimeout = () => {
        self.log('Server conflict timeout. ');
    };

    XHR.send(data);

  }

  private _progressRatio (nbIx: number, nbCompleted: number, nbPending): number {
    const nbTables = this.tablesToSync.length;
    return Math.round( (nbTables - (nbCompleted + nbPending) + nbIx) / nbTables * 100);
  }

  private _encodeBase64(input: string): string {
    let output = '';
    let chr1, chr2, chr3, enc1, enc2, enc3, enc4;
    let i = 0;

    input = this._utf8_encode(input);

    while (i < input.length) {

        chr1 = input.charCodeAt(i++);
        chr2 = input.charCodeAt(i++);
        chr3 = input.charCodeAt(i++);

        enc1 = chr1 >> 2;
        enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
        enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
        enc4 = chr3 & 63;

        if (isNaN(chr2)) {
            enc3 = enc4 = 64;
        } else if (isNaN(chr3)) {
            enc4 = 64;
        }

        output = output +
        this.keyStr.charAt(enc1) + this.keyStr.charAt(enc2) +
        this.keyStr.charAt(enc3) + this.keyStr.charAt(enc4);

    }

    return output;
}

private _utf8_encode(input: string): string {
    input = input.replace(/\r\n/g,'\n');
    let utftext = '';

    for (let n = 0; n < input.length; n++) {

        const c = input.charCodeAt(n);

        if (c < 128) {
            utftext += String.fromCharCode(c);
        }
        else if((c > 127) && (c < 2048)) {
            utftext += String.fromCharCode((c >> 6) | 192);
            utftext += String.fromCharCode((c & 63) | 128);
        }
        else {
            utftext += String.fromCharCode((c >> 12) | 224);
            utftext += String.fromCharCode(((c >> 6) & 63) | 128);
            utftext += String.fromCharCode((c & 63) | 128);
        }

    }

    return utftext;
}

}
