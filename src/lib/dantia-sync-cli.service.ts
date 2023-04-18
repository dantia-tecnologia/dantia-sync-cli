import { Injectable } from '@angular/core';
import { DbWrapperService, DBTable, DBSchema } from 'db-wrapper';
import { SqlTransaction, SqlResultSet, DataToSync, SqlError, DataFromServer, ProgressData,
  SyncInfo, SyncResult, TableToSync, DataRecord, DataOperation, SqlTransactionError,
  DataConflict } from './dantia-sync-cli.models';
import { Observable } from 'rxjs';
import { share} from 'rxjs/operators';

@Injectable()
export class DantiaSyncCliService {
  private db: any;
  private schema: DBSchema;
  private tablesToSync: TableToSync[]  = [];
  private idNameFromTableName = {};
  private sizeMax = 1048576;
  private firstSync: any;
  private clientData;
  private serverData;
  private SqlTranError: SqlTransactionError;
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
  public syncResult: SyncResult;

  constructor(private dbWrp: DbWrapperService) {
    this.db = dbWrp.db;
    this.schema = dbWrp.schema;
    this.syncInfo = {uuid: '', version: '0', lastSyncDate: {}};
    this.firstSync = {};
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
    if (this.syncResult !== undefined) {
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
      return new Promise(resolve => {
        this._selectSql('select count(*) from _change_elem', undefined, undefined, cont => { resolve(cont[0] > 0); } );
      });
    }
  }

  getLastSyncDate(tableName?: string): number {
    if (tableName) {
      if (this.syncInfo.lastSyncDate.hasOwnProperty(tableName)) {
        return this.syncInfo.lastSyncDate[tableName];
      } else {
        throw new Error(`${tableName} not found.`);
      }
    } else {
      let arrLastSyncDate = Object.values(this.syncInfo.lastSyncDate);
      arrLastSyncDate = arrLastSyncDate.filter( (elem: number) => { if (elem !== 0) { return elem; } });
      if (arrLastSyncDate.length === 0) {
        return 0;
      } else {
        return arrLastSyncDate.reduce( (minLast: number, lastSync: number) => {
          if (minLast > lastSync) { return lastSync; } else { return minLast; }
        }, Math.round(+new Date(2050, 11, 31) / 1000));
      }
    }
  }

  setSyncDate(tableName: string, val: number) {
    if (this.syncInfo.lastSyncDate.hasOwnProperty(tableName)) {
      this.syncInfo.lastSyncDate[tableName] = val;
      this._executeSql('UPDATE _sync_info SET last_sync = ? where table_name = ?', [val, tableName]);
    } else {
      throw new Error(`${tableName} not found.`);
    }
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

  syncNow(modelsToSync?: string[] | string, saveBandwidth?: boolean): Observable<SyncResult> {
    const self = this;
    let modelsToBackup: string[] = [];
    if (modelsToSync) {
      modelsToBackup = this.checkModelsList(modelsToSync);
    }
    if (this.db === null) {
        self.log('You should call the initSync before (db is null)');
        throw new Error ('You should call the initSync before (db is null)');
    }

    return Observable.create ( observer => {
      self.cbEndSync = () => {
        if (self.syncResult)
          this.callBackProgress(self.syncResult.message, 100, self.syncResult.codeStr);
        const resultado = self.syncResult;
        self.syncResult = undefined;
        observer.next(resultado);
        observer.complete();
      };

      self.callBackProgress = (message, percent, position) => {
        this.progressObserver.next({message, percent, position});
      };

      if (self.syncResult !== undefined) {
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
          };
          // self.syncResult.models = {pendiente = [], completado = []};
          if (modelsToBackup.length === 0) {
            self.tablesToSync.forEach( table => {
                modelsToBackup.push(table.tableName);
            });
          }
          this.callBackProgress('Getting local data to backup', 0, 'getData');

          self.syncDate = Math.round(new Date().getTime() / 1000.0);
          self._syncNowGo(modelsToBackup, this.callBackProgress, saveBandwidth);
      }
    });
  }

  /******* PRIVATE FUNCTIONS  ******************/

  private init(): Promise<void> {
    console.log('init');
    const  self = this;
    this.schema = this.dbWrp.schema;
    this.db = this.dbWrp.db;
    if (this.schema) {
      this.schema.tables.forEach((table: DBTable) => {
        if (table.sync) {
          this.tablesToSync.push({tableName: table.name, idName: table.sync, ddl: ''});
          this.idNameFromTableName[table.name] = table.sync;
        }
      });
    }
    return new Promise( (resolve, reject) => {
      this.db.transaction( (tx: SqlTransaction) => {
        this._executeSql('CREATE TABLE IF NOT EXISTS _change_elem (table_name TEXT NOT NULL, id TEXT NOT NULL, oper TEXT NOT NULL, ' +
            'change_time TIMESTAMP NOT NULL DEFAULT  (strftime(\'%s\',\'now\')), data TEXT NULL);', [], tx);
        this._executeSql('CREATE INDEX IF NOT EXISTS index_tableName_changeElem on _change_elem (table_name)', [], tx);
        this._executeSql('CREATE TABLE IF NOT EXISTS _sync_info (table_name TEXT NOT NULL, last_sync TIMESTAMP);', [], tx);
        this._executeSql('CREATE INDEX IF NOT EXISTS index_tableName_syncInfo on _sync_info (table_name)', [], tx);

        // create triggers to automatically fill the _change_elem table (this table will contains a pointer to all the modified data)
        this.tablesToSync.forEach(curr => {
          self._getListColumns(curr.tableName, tx, function(listColumns) {
            self._executeSql('CREATE TRIGGER IF NOT EXISTS update_' + curr.tableName + '  AFTER UPDATE ON ' + curr.tableName + ' ' +
                      'WHEN (SELECT last_sync FROM _sync_info where table_name = \'' +  curr.tableName + '\') > 0 ' +
                      'BEGIN INSERT INTO _change_elem (table_name, id, oper) VALUES ' +
                      '("' + curr.tableName + '", new.' + curr.idName + ', \'U\'); END;', [], tx);

            const dataInsert = '\'{\' ||'.concat(
              listColumns.map(function(col) {
                return 'case when new.'.concat(col.column, ' is null then \'"',col.column, '": null \' else ',
                  '\'"',col.column, '":', (col.quoted?'"':''),'\' || new.',col.column,(col.quoted?' || \'"\' ':' || \'\''), ' end');
                }).join(' || \',\' || '),' || \'}\''
            );
            self._executeSql('CREATE TRIGGER IF NOT EXISTS insert_' + curr.tableName + '  AFTER INSERT ON ' + curr.tableName + ' ' +
                      'WHEN (SELECT last_sync FROM _sync_info where table_name = \'' +  curr.tableName + '\') > 0 ' +
                      'BEGIN INSERT INTO _change_elem (table_name, id, oper, data) VALUES ' +
                      '("' + curr.tableName + '", new.' + curr.idName + ', \'I\',' + dataInsert + '); END;', [], tx);

            const dataDelete =  '\'{\' ||'.concat(
              listColumns.map(function(col) {
                return 'case when old.'.concat(col.column, ' is null then \'"',col.column, '": null \' else ',
                  '\'"',col.column, '":', (col.quoted?'"':''),'\' || old.',col.column,(col.quoted?' || \'"\' ':' || \'\''), ' end');
              }).join(' || \',\' || '),' || \'}\''
            );
            self._executeSql('CREATE TRIGGER IF NOT EXISTS delete_' + curr.tableName + '  AFTER DELETE ON ' + curr.tableName + ' ' +
                      'BEGIN INSERT INTO _change_elem (table_name, id, oper, data) VALUES ' +
                      '("' + curr.tableName + '", old.' + curr.idName + ', \'D\',' + dataDelete + '); END;', [], tx);

            self._getDDLTable(curr.tableName, tx, (ddl) => { curr.ddl = ddl; });
            self._selectSql('SELECT last_sync FROM _sync_info where table_name = ?', [curr.tableName], tx, res => {
              if (res.length === 0 || res[0] === 0) { // First sync (or data lost)
                if (res.length === 0) {
                  self._executeSql('INSERT OR REPLACE INTO _sync_info (table_name, last_sync) VALUES (?,?)', [curr.tableName, 0], tx);
                }
                self.firstSync[curr.tableName] = true;
                self.syncInfo.lastSyncDate[curr.tableName] = 0;
              } else {
                self.firstSync[curr.tableName] = false;
                self.syncInfo.lastSyncDate[curr.tableName] = res[0];
                if (self.syncInfo.lastSyncDate[curr.tableName] === 0) { self.firstSync[curr.tableName] = true; }
              }
            });
          });
        });

      }, (err: SqlError) => {
        reject(this.SqlTranError);
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
              if (serverData.data.delete_elem) {
                  self.syncResult.nbDeleted = serverData.data.delete_elem.length;
              }
              if (typeof serverData.data === 'undefined' || serverData.data.length === 0) {
                // nothing to update
                // We only use the server date to avoid dealing with wrong date from the client
                self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                self.syncResult.syncOK = true;
                self.syncResult.codeStr = 'syncOk';
                self.syncResult.message = 'First load synchronized successfully. (' + self.syncResult.nbSent +
                    ' new/modified element saved, ' + self.syncResult.nbUpdated + ' updated and ' +
                    self.syncResult.nbDeleted + ' deleted elements.)';
                self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                self.cbEndSync();
                return;
              }
              let sqlErrs: SqlError[] = [];
              let counterNbTable = 0;
              self.serverData = serverData;
              const nbTables = serverData.models.completado.length;
              const nbTablesPdte = serverData.models.pendiente.length;
              const callFinishUpdate =  (table: any, method: string, sqlTableErrs?: SqlError[]) => {
                if (sqlTableErrs) { sqlErrs = sqlErrs.concat(sqlTableErrs); }
                counterNbTable++;
                const perProgress = this._progressRatio(counterNbTable, nbTables, nbTablesPdte);
                this.log(`${table.tableName} finish,  percent: ${perProgress.toString()}`);
                callBackProgress(table.tableName, perProgress, method);
                if (counterNbTable === nbTables) {
                  if (sqlErrs.length > 0) {
                    self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                    self.syncResult.syncOK = false;
                    self.syncResult.codeStr = 'syncKoData';
                    self.syncResult.message = `errors found (${sqlErrs.length}) `;
                    sqlErrs.forEach(err => { self.syncResult.message += err.message + ' '; });
                    self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                    self.cbEndSync();
                  } else if (serverData.models.pendiente.length === 0)  {
                    self.syncResult.localDataUpdated = self.syncResult.nbUpdated > 0;
                    self.syncResult.syncOK = true;
                    self.syncResult.codeStr = 'syncOk';
                    self.syncResult.message = 'First load synchronized successfully. (' + self.syncResult.nbSent +
                        ' new/modified element saved, ' + self.syncResult.nbUpdated + ' updated and ' +
                        self.syncResult.nbDeleted + ' deleted elements.)';
                    self.syncResult.serverAnswer = serverData; // include the original server answer, just in case
                    self.cbEndSync();
                  } else {
                    self._syncNowGo(serverData.models.pendiente, callBackProgress, saveBandwidth);
                  }
                }
              };
              serverData.models.completado.forEach(tableName => {
                const table = this._getTableToProcess(tableName);
                if (table) {
                  const currData = serverData.data[table.tableName] || [];
                  const deleData = serverData.data.delete_elem[table.tableName] || [];
                  if (this.firstSync[table.tableName]) {
                    self._updateFirstLocalDb({ table, currData }, callFinishUpdate);
                  } else {
                    self._updateLocalDb({ table, currData, deleData }, callFinishUpdate);
                  }
                }
              });
          }
        });
    });

  }

  private _getDataToBackup(modelsToBck: string[], dataCallBack: (datas: DataToSync) => void ): void {
    let nbData = 0;
    const self = this;
    this.log('_getDataToBackup');
    const dataToSync: DataToSync = {
        info: JSON.parse(JSON.stringify(this.syncInfo)),
        data: {},
        delete_elem: {}
    };
    self.db.readTransaction( (tx: SqlTransaction) => {
      let counter = 0;
      const nbTables = modelsToBck.length;
      dataToSync.info.lastSyncDate = {};
      modelsToBck.forEach((tableName: string) => { // a simple for will not work here because we have an asynchronous call inside
        dataToSync.info.lastSyncDate[tableName] = this.syncInfo.lastSyncDate[tableName];
        const currTable: TableToSync | undefined = self._getTableToProcess(tableName);
        if (currTable) {
          this._getDataToSavDel(currTable.tableName, currTable.idName, this.firstSync[currTable.tableName], tx, data => {
            dataToSync.data[tableName] = data;
            nbData += data.length;
            counter++;
            if (counter === nbTables) {
              this.log('Data fetched from the local DB');
              this.syncResult.nbSent += nbData;
              dataCallBack(dataToSync);
            }
          });
        } else {
          counter++;
          if (counter === nbTables) {
            this.log('Data fetched from the local DB');
            this.syncResult.nbSent += nbData;
            dataCallBack(dataToSync);
          }
        }
      }); // end for each

    }, (err: SqlError) => {
      self.log('TransactionError: _getDataToBackup');
      self._errorHandler(undefined , err);
    }, () => {
      self.log('TransactionFinish: _getDataToBackup');
    });
  }

  private _finishSync(tableName: string, syncDate: number, tx: SqlTransaction): void {
    var self = this;
    this.firstSync[tableName] = false;
    this.syncInfo.lastSyncDate[tableName] = syncDate;
    this._executeSql('UPDATE _sync_info SET last_sync = ? where  table_name = ?', [syncDate, tableName], tx);

    if (this.clientData.data.hasOwnProperty(tableName)) {
      const idsDelToDelete: string[] = [];
      let idsString = '';
      const idName =  this.idNameFromTableName[tableName];
      this.clientData.data[tableName].forEach(reg => {
        if (reg.TipoOper === 'U' || reg.TipoOper === 'I') {
          idsDelToDelete.push(reg[idName]);
          } else {
              idsDelToDelete.push(reg.IdOper);
          }
      });

      if (idsDelToDelete.length > 0) {
        idsString = idsDelToDelete.map(x => '?').join(',');
        this._executeSql(`DELETE FROM _change_elem WHERE table_name = '${tableName}'
            AND id IN (${idsString})
            AND change_time <= ${syncDate}`, idsDelToDelete, tx);
      }
    }
  }

  private _getTableToProcess(tableName: string): TableToSync | undefined {

    let result: TableToSync | undefined = this.tablesToSync.find( table => {
        if (table.tableName === tableName) { return table;  }
    });
    if (!result) {
      this.error(tableName + ' no se encuentra entre las tablas a sincronizar.');
    }
    return result;
  }

  private _getDataToSavDel(tableName: string, idName: string, needAllData: boolean, tx: SqlTransaction,
                           dataCallBack: (data: object[]) => void ): void {
      const sql = 'select distinct op.oper TipoOper, op.id IdOper, op.data, op.change_time DateOper, c.* ' +
        'from ( select op.oper, op.id, max(op.change_time) change_time from  _change_elem op ' +
        'where op.table_name= ? AND op.change_time <= ? group by op.oper, op.id) opmod ' +
        'inner join _change_elem op on op.oper = opmod.oper and op.id = opmod.id ' +
        '   and op.change_time = opmod.change_time ' +
        'left join ' + tableName + ' c on c.' + idName + ' = op.id ' +
        'where op.table_name= ? AND op.change_time <= ? ' +
        'order by op.change_time, case when op.oper = \'I\' then 1 ' +
        ' when op.oper = \'U\' then 2 when op.oper = \'D\' then 3 end' ;

      this._selectSql(sql, [tableName, this.syncDate, tableName, this.syncDate], tx, function(data: any[]) {
        var result = data.map(function (elem: any) {
          if (elem.TipoOper === 'I' || elem.TipoOper === 'D') {
            var data = JSON.parse(elem.data);
            Object.keys(data).forEach(function (field) {
              if (Object.getOwnPropertyDescriptor(elem,field)) {
                elem[field] = data[field];
              }
            });
          }
          delete elem.data;
          return elem;
        });
        dataCallBack(result);
      });
  }

  private _detectConflict(tableName: string, idValue: any, tx: SqlTransaction, callBack: (detect: boolean) => void): void  {
    let sql: string;
    const self = this;
    if (!this.firstSync[tableName]) {
        sql = 'select DISTINCT id FROM _change_elem ' +
          ' WHERE table_name = ?  AND id = ? AND change_time > ? ';

        self._selectSql(sql, [tableName, idValue, this.syncDate, tableName, idValue, this.syncDate], tx,
          (exists) => {
            if (exists.length) { callBack(true); } else { callBack(false); }
          });
    } else {
      callBack(false);
    }
  }

  private _updateRecord(tableName: string, idName: string, reg: object, tx: SqlTransaction, callBack?: (tx: SqlTransaction, sqlErr?: SqlError) => void ) {
    let sql: string;
    const self = this;

    this._detectConflict(tableName, reg[idName], tx, exists => {
        if (!exists) {
            /*ex : UPDATE "tableName" SET colonne 1 = [valeur 1], colonne 2 = [valeur 2]*/
            const attList = this._getAttributesList(tableName, reg);
            sql = this._buildUpdateSQL(tableName, reg, attList);
            sql += ' WHERE ' + idName + ' = ? ';
            const attValue = this._getMembersValue(reg, attList);

            self._executeSql(sql, [ ...attValue, reg[idName]], tx, () => {
                sql = 'DELETE FROM _change_elem WHERE ' +
                    'table_name = ? AND id = ? AND ' +
                    'change_time = (select MAX(change_time) FROM _change_elem  ' +
                    'WHERE table_name = ?  AND id = ?) ';

                self._executeSql(sql, [tableName, reg[idName], tableName, reg[idName] ], tx,
                  () => {
                    self.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Updated});
                    if (callBack && typeof callBack === 'function') { callBack(tx); }
                  },
                  (ts, error) => {
                    self._errorHandler(ts, error);
                    if (callBack && typeof callBack === 'function') { callBack(tx, error); }
                  });
            });

        } else {  // send conflict to server
          if (callBack && typeof callBack === 'function') { callBack(tx); }
          self._sendConflict(tableName, idName, reg, tx);
        }
    });
  }

  private _updateLocalDb(serverData: any, callBack: (table: any, method: string, sqlErrs?: SqlError[]) => void): void {
    const sqlErrs: SqlError[] = [];
    const table = serverData.table;
    const currData = serverData.currData;
    const deleData = serverData.deleData;
    let counterNbElm = 0;
    const nb = currData.length;
    const nbDel = deleData.length;
    counterNbElm += nb;
    this.log(`There are ${nb} new or modified elements and ${nbDel} deleted, in the table ${table.tableName}to save in the local DB`);
    let counterNbElmTab = 0;

    const callOperation = (tx: SqlTransaction, err?: SqlError ) => {
      counterNbElmTab++;
      if (err) { sqlErrs.push(err); }
      if (counterNbElmTab === nb ) {
        this._finishSync(table.tableName, this.serverData.syncDate, tx);
      }
    };

    this.db.transaction((tx: SqlTransaction) => {
      this._deleteTableLocalDb (table.tableName, table.idName, deleData, tx,  () => {
        const listIdToCheck: string[] = [];
        if (nb !== 0) {
          for (let i = 0; i < nb; i++) {
              listIdToCheck.push(currData[i][table.idName]);
          }
          this._getIdExitingInDB(table.tableName, table.idName, listIdToCheck, tx, idInDb => {
            let curr;
            for (let i = 0; i < nb; i++) {
              curr = currData[i];

              if (idInDb.indexOf(curr[table.idName]) !== -1) {// update
                this._updateRecord(table.tableName, table.idName, curr, tx, callOperation);
              } else {// insert
                this._insertRecord(table.tableName, table.idName, curr, tx, callOperation);
              }

            } // end for
          }); // end getExisting Id
        } else  { this._finishSync(table.tableName, this.serverData.syncDate, tx); }
      }); // end delete elements
    }, (err) => {
      this.log(`TransactionError (${table.tableName}): ${err.message}`);
      sqlErrs.push(err);
      this._errorHandler(undefined, err);
      delete this.clientData.data[table.tableName]; // this.clientData = null;
      delete this.serverData.data[table.tableName];  // this.serverData = null;
      callBack(table, 'updateLocalDb', sqlErrs);
    }, () => {
      if (sqlErrs.length === 0) { callBack(table, 'updateLocalDb'); } else { callBack(table, 'updateLocalDb', sqlErrs); }
    }); // end tx
  }

  private _updateFirstLocalDb(serverData: any, callBack: (table: any, method: string, sqlErrs?: SqlError[]) => void): void {
    const sqlErrs: SqlError[] = [];
    const table = serverData.table;
    const currData = serverData.currData;
    let counterNbElm = 0;
    const nb = currData.length;
    counterNbElm += nb;
    this.log('There are ' + nb + ' new elements, in the table ' + table.tableName + ' to save in the local DB');

    let counterNbElmTab = 0;
    this.db.transaction ( (tx: SqlTransaction) => {
      if (nb !== 0) {
        for (let i = 0; i < nb; i++) {
          this._insertRecord(table.tableName, table.idName, currData[i], tx, (tx2, err) => {
            counterNbElmTab++;
            if (err) { sqlErrs.push(err); }
            if (counterNbElmTab === nb) {
              this._finishSync(table.tableName, this.serverData.syncDate, tx2);
            }
          });
        }
      } else {
        this._finishSync(table.tableName, this.serverData.syncDate, tx);
      }
    }, (err) => {
      this.log(`TransactionError (${table.tableName}): ${err.message}`);
      sqlErrs.push(err);
      this._errorHandler(undefined, err);
      callBack(table, 'updateFirstLocalDb', sqlErrs);
    }, () => {
      this.syncResult.nbUpdated += counterNbElmTab;
      if (sqlErrs.length === 0) { callBack(table, 'updateFirstLocalDb'); }  else { callBack(table, 'updateFirstLocalDb', sqlErrs); }
    });

  }

  private _insertRecord(tableName: string, idName: string, reg: object, tx: SqlTransaction, callBack?: (tx: SqlTransaction, sqlErr?: SqlError) => void) {
    let sql: string;
    const self = this;

    this._detectConflict(tableName, reg[idName], tx,
      (exists) => {
        if (!exists) {

            // 'ex INSERT INTO tableName (id, name, type, etc) VALUES (?, ?, ?, ?);'
            const attList = self._getAttributesList(tableName, reg);
            sql = self._buildInsertSQL(tableName, reg, attList);
            const attValue = self._getMembersValue(reg, attList);
            if (!self.firstSync[tableName]) {
                self._executeSql(sql, attValue, tx, () => {
                    sql = 'DELETE FROM _change_elem WHERE ' +
                        'table_name = ? AND id = ? AND ' +
                        'change_time = (select MAX(change_time) FROM _change_elem WHERE ' +
                        'table_name = ? AND id = ?) ';

                    self._executeSql(sql, [tableName, reg[idName], tableName, reg[idName]], tx,
                      () => {
                        this.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Inserted});
                        if (callBack && typeof callBack === 'function') { callBack (tx); }
                      });
                }, (ts, error) => {
                  self._errorHandler(ts, error);
                  if (callBack && typeof callBack === 'function') { callBack(tx, error); }
                });
            } else {
                self._executeSql(sql, attValue, tx,
                  () => {
                    this.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Inserted});
                    if (callBack && typeof callBack === 'function') { callBack (tx); }
                  },
                  (ts, error) => {
                    self._errorHandler(ts, error);
                    if (callBack && typeof callBack === 'function') { callBack(tx, error); }
                });
            }
        } else {  // send conflict to server
            self._sendConflict(tableName, idName, reg, tx);
        }
    });
  }

  private _sendConflict(tableName: string, idName: any, reg: object, tx: SqlTransaction): void {
    const self = this;
    let sql: string ;
    let client;

    sql = 'select * FROM ' + tableName + ' WHERE ' + idName + ' = ?';
    self._selectSql(sql, [reg[idName]], tx, regloc => {

        if (regloc.length === 0) {
            client = 'DELETED';
        } else {
            client = regloc;
        }
        const dataToSend: DataConflict = {
          info: self.syncInfo,
          client: client,
          server: reg
        };
        self._sendConflictToServer(dataToSend);
    });
  }

  private _transformRs(rs: SqlResultSet): object[] {
    const elms: Object[] = [];
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

  private _deleteTableLocalDb(tableName: string, idName: string, listIdToDelete: any[],
                              tx: SqlTransaction, callBack: (final: boolean) => void): void {

    const listIds: any[] = [];
    let orden = 0;
    if (listIdToDelete.length === 0) {
      callBack(true);
    } else {
      listIds.push(
        listIdToDelete.reduce((listIdsDel, id, ix) => {
          if ((ix % 50) === 0 && listIdsDel.length !== 0) {
            listIds.push(listIdsDel);
            listIdsDel = [];
          }
          listIdsDel.push(id);
          return listIdsDel;
        }, [])
      );

      listIds.map((listIdsDel, ix, list) => {
        this._deleteParcialTableLocalDb(tableName, idName, listIdsDel, tx, () => {
          if (++orden === list.length) {
            callBack(true);
          }
        });
      });
    }
  }

  private _deleteParcialTableLocalDb(tableName: string, idName: string, listIdToDelete: any[],
                                     tx: SqlTransaction, callBack: (final: boolean) => void): void {
    const self = this;

    let  sql = `delete from ${tableName} WHERE ${idName} IN (${listIdToDelete.map(x => '?').join(',')})`;
    this._executeSql(sql, listIdToDelete, tx, () => {
      sql = `delete from _change_elem WHERE table_name = "${tableName}" and id  IN (${listIdToDelete.map(x => '?').join(',')})`;
      self._executeSql(sql, listIdToDelete, tx, () => {
        const reg = {};
        listIdToDelete.forEach( x => {
          reg[idName] = x;
          self.dataObserver.next({table: tableName, record: reg, operation: DataOperation.Deleted});
        });
        callBack(true);
      });
    });
  }

  private _getIdExitingInDB(tableName: string, idName: string, listIdToCheck: any[], tx: SqlTransaction,
                            dataCallBack: (IdExiting: any[]) => void): void {

    const listIds: any[] = [];
    let idsInDb: any[] = [];
    let orden = 0;
    if (listIdToCheck.length === 0) {
      dataCallBack([]);
    } else {
      listIds.push(
        listIdToCheck.reduce((listIdsCheck, id, ix) => {
          if ((ix % 50) === 0 && listIdsCheck.length !== 0) {
            listIds.push(listIdsCheck);
            listIdsCheck = [];
          }
          listIdsCheck.push(id);
          return listIdsCheck;
        }, [])
      );

      listIds.map((listIdsCheck, ix, list) => {
        this._getParcialIdExitingInDB(tableName, idName, listIdsCheck, tx, (idsFind) => {
          idsInDb = idsInDb.concat(idsFind);
          if (++orden === list.length) {
            dataCallBack(idsInDb);
          }
        });
      });
    }
  }

  private _getParcialIdExitingInDB(tableName: string, idName: string, listIdToCheck: any[], tx: SqlTransaction,
                                   dataCallBack: (IdExiting: any[]) => void): void {
    const sql = `select ${idName} FROM ${tableName} WHERE ${idName} IN ( ${listIdToCheck.map(x => '?').join(',')} )`;
    this._selectSql(sql, listIdToCheck, tx, (idsFind) =>  {
        dataCallBack(idsFind);
    });
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
            this.SqlTranError = {message: error.message, code: error.code, sql};
            return errorHandler(transaction, error);
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
        optionalErrorHandler = this._errorHandler;
    }
    if (optionalTransaction) {
        this._executeSqlBridge(optionalTransaction, sql, params || [], optionalCallBack, optionalErrorHandler);
    } else {
      if (sql.indexOf('select') === 0) {
        this.db.readTransaction ((tx: SqlTransaction) => {
            self._executeSqlBridge(tx, sql, params, optionalCallBack, optionalErrorHandler);
        });
      } else {
        this.db.transaction ((tx: SqlTransaction) => {
          self._executeSqlBridge(tx, sql, params, optionalCallBack, optionalErrorHandler);
        });
      }
    }
  }


  private _defaultCallBack(transaction: SqlTransaction, results: SqlResultSet) {
  // DBSYNC.log('SQL Query executed. insertId: '+results.insertId+' rows.length '+results.rows.length);
  }

  private _selectSql(sql: string, params?: any[], optionalTransaction?: SqlTransaction,
                     callBack?: (result: object[] | any[]) => void): void {
    const self = this;
    this._executeSql(sql, params, optionalTransaction,
      (tx, rs) => { if (callBack && typeof callBack === 'function') { callBack(self._transformRs(rs)); }});
  }

  private _errorHandler = (transaction: SqlTransaction, error: SqlError) => {
    // this.log(error);
    this.error('Error : ' + error.message + ' (Code ' + error.code + ')' );
    return true;
  }

  private _buildInsertSQL(tableName: string, objToInsert: object, attrList?: any[]): string {
    let members;
    if (attrList) { members = attrList;
    } else { members = this._getAttributesList(tableName, objToInsert);  }
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

  private _buildUpdateSQL(tableName: string, objToUpdate: object, attrList?: any[]): string {
  /*ex UPDATE "nom de table" SET colonne 1 = [valeur 1], colonne 2 = [valeur 2] WHERE {condition}*/
    let members;
    let sql = 'UPDATE ' + tableName + ' SET ';
    if (attrList) { members = attrList;
    } else {
      members = this._getAttributesList(tableName, objToUpdate);
    }
    if (members.length === 0) {
        throw new Error('buildUpdateSQL : Error, try to insert an empty object in the table ' + tableName);
    }

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


  private _getMembersValue(obj: object, members: string[]): any[] {
    const memberArray: any[] = [];
    members.forEach( member => {
      memberArray.push(obj[member]);
    });
    return memberArray;
  }

  private _getAttributesList(tableName: string, obj: object, check?: any): any[] {
    const memberArray = [];
    const table = this._getTableToProcess(tableName);
    for (const elm in obj) {
        if (check && typeof this[elm] === 'function' && !obj.hasOwnProperty(elm)) {
            continue;
        }
        if (table.ddl.indexOf(elm) === -1) { continue; } else { memberArray.push(elm); }
    }
    return memberArray;
  }

  private _getMembersValueString(obj: object, members: string[], separator: string): string {
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

    const XHR = new XMLHttpRequest();
    const data = JSON.stringify(dataToSync);
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
            } catch (e) {
                serverAnswer = XHR.responseText;
            }
            self.log('Server answered: ');
            self.log(JSON.stringify({result: serverAnswer.result, message: serverAnswer.message}));
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
        const serverAnswer = {
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

    if (self.username !== null && self.password !== null && self.username !== undefined && self.password !== undefined ) {
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

  private _columnExists(table: string, column: string, optionalTransaction, callback: (response: boolean) => void) {
    const self = this;
    self._getDDLTable(table, optionalTransaction,
      (ddl) => {
        if (ddl.indexOf(column) === -1) { callback(false); } else {  callback(true); }
      });
  }

  private _getDDLTable(table: string, optionalTransaction, callback: (response: string) => void) {
    const self = this;
    const sql = 'select sql from sqlite_master where type=\'table\' and name=\'' + table + '\'';
    self._selectSql(sql, [], optionalTransaction, (rs) => { callback(rs[0]); });
  }

  private _getListColumns(table: string, optionalTransaction, callback:(response: any[]) => void) {
    const self = this;
    self._getDDLTable(table, optionalTransaction, function(ddl) {
      const regexlistColumns = /([A-Z0-9_]+)\s([A-Z0-9_]+)\s?(?:NOT NULL\s)?(?:\sDEFAULT\s[\w\'\(\)]+\s)?(?:\s+CONSTRAINT\s[A-Z0-9_]+\s)?(?:CHECK\s[A-Z0-9_\(\)<=\s]+)?\,/gi;
      const result = [];
      let listColumns = regexlistColumns.exec(ddl);
      if (!listColumns) {
        throw new Error('regex: Imposible obtener la lista de campos.');
      }
      while (listColumns !== null) {
        var column = listColumns[1], type = listColumns[2];
        if (column !== 'conflict') {
          var quoted = false;
          if (type.toLowerCase() === 'text') { quoted = true; }
          result.push({ column: column, quoted: quoted });
        }
        listColumns = regexlistColumns.exec(ddl);
      }
      callback(result);
    });
  }

  private checkModelsList(tableList: string[] | string): string[] {
    let listToCheck: string[] = [];
    const tablesToSync = this.tablesToSync.map( t => t.tableName);
    if (tableList instanceof Array) {
      listToCheck = tableList;
    } else {
      listToCheck = tableList.split(',');
    }
    if (listToCheck.some( (t) => tablesToSync.indexOf(t) === -1 ) ) {
      throw new Error ('Any item in the list is invalid.');
    } else {
      return listToCheck;
    }
  }

  private _progressRatio(nbIx: number, nbCompleted: number, nbPending): number {
    const nbTables = this.tablesToSync.length;
    return Math.round( (nbTables - (nbCompleted + nbPending) + nbIx) / nbTables * 100);
  }

  private _encodeBase64(input: string): string {
    let output = '';
    // tslint:disable-next-line:one-variable-per-declaration
    let chr1, chr2, chr3, enc1, enc2, enc3, enc4;
    let i = 0;

    input = this._utf8_encode(input);

    while (i < input.length) {

        chr1 = input.charCodeAt(i++);
        chr2 = input.charCodeAt(i++);
        chr3 = input.charCodeAt(i++);
        // tslint:disable:no-bitwise
        enc1 = chr1 >> 2;
        enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
        enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
        enc4 = chr3 & 63;
        // tslint:enable:no-bitwise
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
    input = input.replace(/\r\n/g, '\n');
    let utftext = '';

    for (let n = 0; n < input.length; n++) {

        const c = input.charCodeAt(n);
        // tslint:disable:no-bitwise
        if (c < 128) {
            utftext += String.fromCharCode(c);
        } else if ((c > 127) && (c < 2048)) {
            utftext += String.fromCharCode((c >> 6) | 192);
            utftext += String.fromCharCode((c & 63) | 128);
        } else {
            utftext += String.fromCharCode((c >> 12) | 224);
            utftext += String.fromCharCode(((c >> 6) & 63) | 128);
            utftext += String.fromCharCode((c & 63) | 128);
        }
        // tslint:enable:no-bitwise
    }

    return utftext;
}

}
