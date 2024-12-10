/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.common.mysql;

import java.util.HashMap;
import java.util.Map;

import static io.dingodb.common.mysql.error.ErrorCode.*;

public class State {

    public static Map<Integer, String> mysqlState = new HashMap<>();

    static {
        mysqlState.put(ErrDupKey,                              "23000");
        mysqlState.put(ErrOutofMemory,                         "HY001");
        mysqlState.put(ErrOutOfSortMemory,                     "HY001");
        mysqlState.put(ErrConCount,                            "08004");
        mysqlState.put(ErrBadHost,                             "08S01");
        mysqlState.put(ErrHandshake,                           "08S01");
        mysqlState.put(ErrDBaccessDenied,                      "42000");
        mysqlState.put(ErrAccessDenied,                        "28000");
        mysqlState.put(ErrNoDB,                                "3D000");
        mysqlState.put(ErrUnknownCom,                          "08S01");
        mysqlState.put(ErrBadNull,                             "23000");
        mysqlState.put(ErrBadDB,                               "42000");
        mysqlState.put(ErrTableExists,                         "42S01");
        mysqlState.put(ErrBadTable,                            "42S02");
        mysqlState.put(ErrNonUniq,                             "23000");
        mysqlState.put(ErrServerShutdown,                      "08S01");
        mysqlState.put(ErrBadField,                            "42S22");
        mysqlState.put(ErrFieldNotInGroupBy,                   "42000");
        mysqlState.put(ErrWrongSumSelect,                      "42000");
        mysqlState.put(ErrWrongGroupField,                     "42000");
        mysqlState.put(ErrWrongValueCount,                     "21S01");
        mysqlState.put(ErrTooLongIdent,                        "42000");
        mysqlState.put(ErrDupFieldName,                        "42S21");
        mysqlState.put(ErrDupKeyName,                          "42000");
        mysqlState.put(ErrDupEntry,                            "23000");
        mysqlState.put(ErrWrongFieldSpec,                      "42000");
        mysqlState.put(ErrParse,                               "42000");
        mysqlState.put(ErrEmptyQuery,                          "42000");
        mysqlState.put(ErrNonuniqTable,                        "42000");
        mysqlState.put(ErrInvalidDefault,                      "42000");
        mysqlState.put(ErrMultiplePriKey,                      "42000");
        mysqlState.put(ErrTooManyKeys,                         "42000");
        mysqlState.put(ErrTooManyKeyParts,                     "42000");
        mysqlState.put(ErrTooLongKey,                          "42000");
        mysqlState.put(ErrKeyColumnDoesNotExits,               "42000");
        mysqlState.put(ErrBlobUsedAsKey,                       "42000");
        mysqlState.put(ErrTooBigFieldlength,                   "42000");
        mysqlState.put(ErrWrongAutoKey,                        "42000");
        mysqlState.put(ErrForcingClose,                        "08S01");
        mysqlState.put(ErrIpsock,                              "08S01");
        mysqlState.put(ErrNoSuchIndex,                         "42S12");
        mysqlState.put(ErrWrongFieldTerminators,               "42000");
        mysqlState.put(ErrBlobsAndNoTerminated,                "42000");
        mysqlState.put(ErrCantRemoveAllFields,                 "42000");
        mysqlState.put(ErrCantDropFieldOrKey,                  "42000");
        mysqlState.put(ErrBlobCantHaveDefault,                 "42000");
        mysqlState.put(ErrWrongDBName,                         "42000");
        mysqlState.put(ErrWrongTableName,                      "42000");
        mysqlState.put(ErrTooBigSelect,                        "42000");
        mysqlState.put(ErrUnknownProcedure,                    "42000");
        mysqlState.put(ErrWrongParamcountToProcedure,          "42000");
        mysqlState.put(ErrUnknownTable,                        "42S02");
        mysqlState.put(ErrFieldSpecifiedTwice,                 "42000");
        mysqlState.put(ErrUnsupportedExtension,                "42000");
        mysqlState.put(ErrTableMustHaveColumns,                "42000");
        mysqlState.put(ErrUnknownCharacterSet,                 "42000");
        mysqlState.put(ErrTooBigRowsize,                       "42000");
        mysqlState.put(ErrWrongOuterJoin,                      "42000");
        mysqlState.put(ErrNullColumnInIndex,                   "42000");
        mysqlState.put(ErrPasswordAnonymousUser,               "42000");
        mysqlState.put(ErrPasswordNotAllowed,                  "42000");
        mysqlState.put(ErrPasswordNoMatch,                     "42000");
        mysqlState.put(ErrWrongValueCountOnRow,                "21S01");
        mysqlState.put(ErrInvalidUseOfNull,                    "22004");
        mysqlState.put(ErrRegexp,                              "42000");
        mysqlState.put(ErrMixOfGroupFuncAndFields,             "42000");
        mysqlState.put(ErrNonexistingGrant,                    "42000");
        mysqlState.put(ErrTableaccessDenied,                   "42000");
        mysqlState.put(ErrColumnaccessDenied,                  "42000");
        mysqlState.put(ErrIllegalGrantForTable,                "42000");
        mysqlState.put(ErrGrantWrongHostOrUser,                "42000");
        mysqlState.put(ErrNoSuchTable,                         "42S02");
        mysqlState.put(ErrNonexistingTableGrant,               "42000");
        mysqlState.put(ErrNotAllowedCommand,                   "42000");
        mysqlState.put(ErrSyntax,                              "42000");
        mysqlState.put(ErrAbortingConnection,                  "08S01");
        mysqlState.put(ErrNetPacketTooLarge,                   "08S01");
        mysqlState.put(ErrNetReadErrorFromPipe,                "08S01");
        mysqlState.put(ErrNetFcntl,                            "08S01");
        mysqlState.put(ErrNetPacketsOutOfOrder,                "08S01");
        mysqlState.put(ErrNetUncompress,                       "08S01");
        mysqlState.put(ErrNetRead,                             "08S01");
        mysqlState.put(ErrNetReadInterrupted,                  "08S01");
        mysqlState.put(ErrNetErrorOnWrite,                     "08S01");
        mysqlState.put(ErrNetWriteInterrupted,                 "08S01");
        mysqlState.put(ErrTooLongString,                       "42000");
        mysqlState.put(ErrTableCantHandleBlob,                 "42000");
        mysqlState.put(ErrTableCantHandleAutoIncrement,        "42000");
        mysqlState.put(ErrWrongColumnName,                     "42000");
        mysqlState.put(ErrWrongKeyColumn,                      "42000");
        mysqlState.put(ErrDupUnique,                           "23000");
        mysqlState.put(ErrBlobKeyWithoutLength,                "42000");
        mysqlState.put(ErrPrimaryCantHaveNull,                 "42000");
        mysqlState.put(ErrTooManyRows,                         "42000");
        mysqlState.put(ErrRequiresPrimaryKey,                  "42000");
        mysqlState.put(ErrKeyDoesNotExist,                     "42000");
        mysqlState.put(ErrCheckNoSuchTable,                    "42000");
        mysqlState.put(ErrCheckNotImplemented,                 "42000");
        mysqlState.put(ErrCantDoThisDuringAnTransaction,       "25000");
        mysqlState.put(ErrNewAbortingConnection,               "08S01");
        mysqlState.put(ErrMasterNetRead,                       "08S01");
        mysqlState.put(ErrMasterNetWrite,                      "08S01");
        mysqlState.put(ErrTooManyUserConnections,              "42000");
        mysqlState.put(ErrReadOnlyTransaction,                 "25000");
        mysqlState.put(ErrNoPermissionToCreateUser,            "42000");
        mysqlState.put(ErrLockDeadlock,                        "40001");
        mysqlState.put(ErrNoReferencedRow,                     "23000");
        mysqlState.put(ErrRowIsReferenced,                     "23000");
        mysqlState.put(ErrConnectToMaster,                     "08S01");
        mysqlState.put(ErrWrongNumberOfColumnsInSelect,        "21000");
        mysqlState.put(ErrUserLimitReached,                    "42000");
        mysqlState.put(ErrSpecificAccessDenied,                "42000");
        mysqlState.put(ErrNoDefault,                           "42000");
        mysqlState.put(ErrWrongValueForVar,                    "42000");
        mysqlState.put(ErrWrongTypeForVar,                     "42000");
        mysqlState.put(ErrCantUseOptionHere,                   "42000");
        mysqlState.put(ErrNotSupportedYet,                     "42000");
        mysqlState.put(ErrWrongFkDef,                          "42000");
        mysqlState.put(ErrOperandColumns,                      "21000");
        mysqlState.put(ErrSubqueryNo1Row,                      "21000");
        mysqlState.put(ErrIllegalReference,                    "42S22");
        mysqlState.put(ErrDerivedMustHaveAlias,                "42000");
        mysqlState.put(ErrSelectReduced,                       "01000");
        mysqlState.put(ErrTablenameNotAllowedHere,             "42000");
        mysqlState.put(ErrNotSupportedAuthMode,                "08004");
        mysqlState.put(ErrSpatialCantHaveNull,                 "42000");
        mysqlState.put(ErrCollationCharsetMismatch,            "42000");
        mysqlState.put(ErrWarnTooFewRecords,                   "01000");
        mysqlState.put(ErrWarnTooManyRecords,                  "01000");
        mysqlState.put(ErrWarnNullToNotnull,                   "22004");
        mysqlState.put(ErrWarnDataOutOfRange,                  "22003");
        mysqlState.put(WarnDataTruncated,                      "01000");
        mysqlState.put(ErrWrongNameForIndex,                   "42000");
        mysqlState.put(ErrWrongNameForCatalog,                 "42000");
        mysqlState.put(ErrUnknownStorageEngine,                "42000");
        mysqlState.put(ErrTruncatedWrongValue,                 "22007");
        mysqlState.put(ErrSpNoRecursiveCreate,                 "2F003");
        mysqlState.put(ErrSpAlreadyExists,                     "42000");
        mysqlState.put(ErrSpDoesNotExist,                      "42000");
        mysqlState.put(ErrSpLilabelMismatch,                   "42000");
        mysqlState.put(ErrSpLabelRedefine,                     "42000");
        mysqlState.put(ErrSpLabelMismatch,                     "42000");
        mysqlState.put(ErrSpUninitVar,                         "01000");
        mysqlState.put(ErrSpBadselect,                         "0A000");
        mysqlState.put(ErrSpBadreturn,                         "42000");
        mysqlState.put(ErrSpBadstatement,                      "0A000");
        mysqlState.put(ErrUpdateLogDeprecatedIgnored,          "42000");
        mysqlState.put(ErrUpdateLogDeprecatedTranslated,       "42000");
        mysqlState.put(ErrQueryInterrupted,                    "70100");
        mysqlState.put(ErrSpWrongNoOfArgs,                     "42000");
        mysqlState.put(ErrSpCondMismatch,                      "42000");
        mysqlState.put(ErrSpNoreturn,                          "42000");
        mysqlState.put(ErrSpNoreturnend,                       "2F005");
        mysqlState.put(ErrSpBadCursorQuery,                    "42000");
        mysqlState.put(ErrSpBadCursorSelect,                   "42000");
        mysqlState.put(ErrSpCursorMismatch,                    "42000");
        mysqlState.put(ErrSpCursorAlreadyOpen,                 "24000");
        mysqlState.put(ErrSpCursorNotOpen,                     "24000");
        mysqlState.put(ErrSpUndeclaredVar,                     "42000");
        mysqlState.put(ErrSpFetchNoData,                       "02000");
        mysqlState.put(ErrSpDupParam,                          "42000");
        mysqlState.put(ErrSpDupVar,                            "42000");
        mysqlState.put(ErrSpDupCond,                           "42000");
        mysqlState.put(ErrSpDupCurs,                           "42000");
        mysqlState.put(ErrSpSubselectNyi,                      "0A000");
        mysqlState.put(ErrStmtNotAllowedInSfOrTrg,             "0A000");
        mysqlState.put(ErrSpVarcondAfterCurshndlr,             "42000");
        mysqlState.put(ErrSpCursorAfterHandler,                "42000");
        mysqlState.put(ErrSpCaseNotFound,                      "20000");
        mysqlState.put(ErrDivisionByZero,                      "22012");
        mysqlState.put(ErrIllegalValueForType,                 "22007");
        mysqlState.put(ErrProcaccessDenied,                    "42000");
        mysqlState.put(ErrXaerNota,                            "XAE04");
        mysqlState.put(ErrXaerInval,                           "XAE05");
        mysqlState.put(ErrXaerRmfail,                          "XAE07");
        mysqlState.put(ErrXaerOutside,                         "XAE09");
        mysqlState.put(ErrXaerRmerr,                           "XAE03");
        mysqlState.put(ErrXaRbrollback,                        "XA100");
        mysqlState.put(ErrNonexistingProcGrant,                "42000");
        mysqlState.put(ErrDataTooLong,                         "22001");
        mysqlState.put(ErrSpBadSQLstate,                       "42000");
        mysqlState.put(ErrCantCreateUserWithGrant,             "42000");
        mysqlState.put(ErrSpDupHandler,                        "42000");
        mysqlState.put(ErrSpNotVarArg,                         "42000");
        mysqlState.put(ErrSpNoRetset,                          "0A000");
        mysqlState.put(ErrCantCreateGeometryObject,            "22003");
        mysqlState.put(ErrTooBigScale,                         "42000");
        mysqlState.put(ErrTooBigPrecision,                     "42000");
        mysqlState.put(ErrMBiggerThanD,                        "42000");
        mysqlState.put(ErrTooLongBody,                         "42000");
        mysqlState.put(ErrTooBigDisplaywidth,                  "42000");
        mysqlState.put(ErrXaerDupid,                           "XAE08");
        mysqlState.put(ErrDatetimeFunctionOverflow,            "22008");
        mysqlState.put(ErrRowIsReferenced2,                    "23000");
        mysqlState.put(ErrNoReferencedRow2,                    "23000");
        mysqlState.put(ErrSpBadVarShadow,                      "42000");
        mysqlState.put(ErrSpWrongName,                         "42000");
        mysqlState.put(ErrSpNoAggregate,                       "42000");
        mysqlState.put(ErrMaxPreparedStmtCountReached,         "42000");
        mysqlState.put(ErrNonGroupingFieldUsed,                "42000");
        mysqlState.put(ErrForeignDuplicateKeyOldUnused,        "23000");
        mysqlState.put(ErrCantChangeTxCharacteristics,         "25001");
        mysqlState.put(ErrWrongParamcountToNativeFct,          "42000");
        mysqlState.put(ErrWrongParametersToNativeFct,          "42000");
        mysqlState.put(ErrWrongParametersToStoredFct,          "42000");
        mysqlState.put(ErrDupEntryWithKeyName,                 "23000");
        mysqlState.put(ErrXaRbtimeout,                         "XA106");
        mysqlState.put(ErrXaRbdeadlock,                        "XA102");
        mysqlState.put(ErrFuncInexistentNameCollision,         "42000");
        mysqlState.put(ErrDupSignalSet,                        "42000");
        mysqlState.put(ErrSignalWarn,                          "01000");
        mysqlState.put(ErrSignalNotFound,                      "02000");
        mysqlState.put(ErrSignalException,                     "HY000");
        mysqlState.put(ErrResignalWithoutActiveHandler,        "0K000");
        mysqlState.put(ErrSpatialMustHaveGeomCol,              "42000");
        mysqlState.put(ErrDataOutOfRange,                      "22003");
        mysqlState.put(ErrAccessDeniedNoPassword,              "28000");
        mysqlState.put(ErrTruncateIllegalForeignKey,           "42000");
        mysqlState.put(ErrDaInvalidConditionNumber,            "35000");
        mysqlState.put(ErrForeignDuplicateKeyWithChildInfo,    "23000");
        mysqlState.put(ErrForeignDuplicateKeyWithoutChildInfo, "23000");
        mysqlState.put(ErrCantExecuteInReadOnlyTransaction,    "25006");
        mysqlState.put(ErrAlterOperationNotSupported,          "0A000");
        mysqlState.put(ErrAlterOperationNotSupportedReason,    "0A000");
        mysqlState.put(ErrDupUnknownInIndex,                   "23000");
        mysqlState.put(ErrBadGeneratedColumn,                  "HY000");
        mysqlState.put(ErrUnsupportedOnGeneratedColumn,        "HY000");
        mysqlState.put(ErrGeneratedColumnNonPrior,             "HY000");
        mysqlState.put(ErrDependentByGeneratedColumn,          "HY000");
        mysqlState.put(ErrInvalidJSONText,                     "22032");
        mysqlState.put(ErrInvalidJSONPath,                     "42000");
        mysqlState.put(ErrInvalidJSONData,                     "22032");
        mysqlState.put(ErrInvalidJSONPathWildcard,             "42000");
        mysqlState.put(ErrJSONUsedAsKey,                       "42000");
        mysqlState.put(ErrJSONDocumentNULLKey,                 "22032");
        mysqlState.put(ErrInvalidJSONPathArrayCell,            "42000");
    }

    private State() {
    }
}
