export interface SqlStatement {
  /**
   * @param sql SQL statement to execute.
   * @param params SQL stetement arguments.
   * @param successCallback Called in case of query has been successfully done.
   * @param errorCallback   Called, when query fails. Return false to continue transaction; true or no return to rollback.
   */
  sql: string;
  params?: any[];
  successCallback?(transaction: SqlTransaction, resultSet: SqlResultSet): void;
  errorCallback?(transaction: SqlTransaction, error: SqlError): void;
}

export interface SqlTransaction {
   executeSql(sql: string,
              params?: any[],
              successCallback?: (transaction: SqlTransaction, resultSet: SqlResultSet) => void,
              errorCallback?: (transaction: SqlTransaction, error: SqlError) => any): void;
}

export interface SqlResultSet {
  insertId: number;
  rowsAffected: number;
  rows: SqlResultSetRowList;
}

export interface SqlResultSetRowList {
  length: number;
  item(index: number): Object;
}

export interface SqlError {
  code: number;
  message: string;
}

export interface TableToSync {
  tableName: string;
  idName: string;
}

export interface SyncInfo {
  uuid: string;
  version: string;
  lastSyncDate: number;
  appName?: string;
  sizeMax?: number;
  username?: string;
  password?: string;
}

export interface UserInfo {
  uuid: string;
  version: string;
  username?: string;
  password?: string;
}

export interface SyncProcess {
  completado: string[];
  pendiente: string[];
}

export interface DataFromServer {
  result: string;
  message: string;
  data: any;
  models: any;
  sizeResponse: number;
  syncDate: number;
  status?: number;
  responseText?: any;
}

export interface SyncResult {
  codeStr: string;
  message: string;
  nbDeleted: number;
  nbSent: number;
  nbUpdated: number;
  localDataUpdated: boolean;
  syncOK: boolean;
  serverAnswer: DataFromServer;
}

export interface DataToSync {
  info: SyncInfo;
  data: Object;
  delete_elem: Object;
}

export interface ProgressData {
  message: string;
  percent: number;
  position: string;
}

export enum DataOperation {
  Inserted,
  Updated,
  Deleted
}

export interface DataRecord {
  table: string;
  record: any;
  operation: DataOperation;
}
