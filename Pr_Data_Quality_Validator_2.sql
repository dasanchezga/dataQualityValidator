create or replace Procedure     Pr_Data_Quality_Validator_2(P_Fecha Date Default Trunc(Sysdate-2)) As
  -- ---------------------------------------------------------------------------
  -- Declaracion De Variables Generales
  -- ---------------------------------------------------------------------------
  Lv_Job_Name                     Varchar2(100)          := 'PR_DATA_QUALITY_VALIDATOR';
  Lv_Usuario_Bd                   Constant Varchar2(30)  := 'STG';
  Lv_Job_Owner                    Constant Varchar2(30)  := 'DANIEL SANCHEZ';
  Lv_Job_Type                     Constant Varchar2(30)  := 'PL-SQL';
  Ln_Job_Id                       Constant Number        := 0;
  Ln_Count                        Number                 := 0;
  Ln_Total                        Number                 := 0;
  Lv_Query                        Varchar2(4000)         := Null;
  Lv_Query_Count                  Varchar2(4000)         := Null;
  Ln_Step                         Number                 := 0;
  Lv_Successfull                  Varchar2(1)            := 'P';
  Lv_Comment_                     Varchar2(4000)         := Null;
  Ld_Start_Date                   Date                   := Sysdate;
  Ln_Etl                          Constant Number        := 5;
  Ld_Data_Date                    Constant Date          := To_Date(To_Char(Sysdate,'YYYYMM')||'01','YYYYMMDD');
  Ant_Date                        Varchar2(100)          := Null;
  N                               Number                 := 0;
  Ev_Date                         Varchar2(10)           := Null;
  Ev_PrimerDia                    Varchar2(100)          := Null;
  Ev_UltimoDia                    Varchar2(100)          := Null;
  Ev_DiaMesAnterior               Varchar2(100)          := Null;
  Ev_UltimoDiaMesAnterior         Varchar2(100)          := Null;
  Lv_Tablatrgt                    Varchar2(20)           :='RESULTS_DQ';
  Lv_Esquematrgt                  Varchar2(20)           :='SMY_COL';
  Ln_Prttn                        Number                 :=0;
  
  
Begin

    -- ---------------------------------------------------------------------------
    -- Inicializa Variables.
    -- ---------------------------------------------------------------------------
    Ln_Count                := 0;
    Ln_Total                := 0;
    Lv_Query                := Null;
    Lv_Query_Count          := Null;
    Ev_Date                 := To_Char(P_Fecha, 'YYYYMMDD');
    Ant_Date                := Ev_Date-1;
    Ev_PrimerDia            := TO_CHAR(TRUNC(P_Fecha, 'MM'), 'YYYY-MM-DD');
    Ev_UltimoDia            := TO_CHAR(LAST_DAY(P_Fecha), 'YYYY-MM-DD');
    Ev_DiaMesAnterior       := TO_CHAR(TRUNC(ADD_MONTHS(P_Fecha, -1), 'MM'), 'YYYY-MM-DD');
    Ev_UltimoDiaMesAnterior := TO_CHAR(LAST_DAY(ADD_MONTHS(P_Fecha, -1)), 'YYYY-MM-DD');
    N                       := 0;
    
        
    -- -------------------------------------------------------------------------  
    -- Verifica Columna
    -- -------------------------------------------------------------------------
    
    
    Select  Nvl(Count(1),0)  Into N
    From Dba_Tab_Columns
    Where Owner='STG'
    And Table_Name  ='DATA_QUALITY_TMP'
    And Column_Name='CTRL_MD5';
    
       
    -- -------------------------------------------------------------------------  
    -- Agregar Md5 Para Cada Columna
    -- -------------------------------------------------------------------------     
    Dbms_Output.Put_Line ('Valores ctrl_md5 actualizados');
  
    -- -------------------------------------------------------------------------
    --Insertar Nuevas Reglas En La Tabla Paramétrica De Reglas Historica
    -- -------------------------------------------------------------------------
    Insert Into Parametrizado.Data_Quality_Rules_2(Aws_Accnt, Srvc_Dq, Dtbs, Cntr, Tbl_Nm, Clmn_Nm, Rl_Nm, Dq_Dmnsn, Qlt_Rl, Implmntd_Rl, Trgt, Aws_Rl, Implmntd_Aws_Rl, Ctrl_Md5, Eff_Dt, Ppn_Dt)
    Select Aws_Accnt, Srvc_Dq, Dtbs, Cntr, Tbl_Nm, Clmn_Nm, Rl_Nm, Dq_Dmnsn, Qlt_Rl, Implmntd_Rl, Trgt, Aws_Rl, Implmntd_Aws_Rl,  Stg.Pk_Cntrl_Md5.Fn_Cntrl_Md5 (Rl_Nmbr, Dtbs, Tbl_Nm, Clmn_Nm, Rl_Nm, Qlt_Rl, Implmntd_Rl )As Ctrl_Md5, p_fecha , Sysdate
      From Stg.Data_Quality_Tmp Rt
      Where Not Exists(
      Select 1
      From (Select *
                      From (    
                      Select A.*, Row_Number () Over (Partition By Aws_Accnt, Srvc_Dq, Dtbs, Cntr, Tbl_Nm, A.Clmn_Nm, Rl_Nm Order By  A.Ppn_Dt Desc) Seq_No
                      From Parametrizado.Data_Quality_Rules_2 A
                      ) Where Seq_No=1) Tp
      Where Tp.Ctrl_Md5 = Stg.Pk_Cntrl_Md5.Fn_Cntrl_Md5 (Rt.Rl_Nmbr, Rt.Dtbs,Rt.Tbl_Nm, Rt.Clmn_Nm, Rt.Rl_Nm, Rt.Qlt_Rl, Rt.Implmntd_Rl)
      );
    Dbms_Output.Put_Line ('Reglas añadidas a tabla permanente');
    -- -------------------------------------------------------------------------
    -- Manejo de la TABLA CATALOGO DE REGLAS
    -- -------------------------------------------------------------------------
    BEGIN 
    
      Execute Immediate 'TRUNCATE TABLE PARAMETRIZADO.TABLES_DQ drop storage';
      Dbms_Output.Put_Line ('Tabla catalogo temporal borrada');
      
      Begin
        Execute Immediate '
          INSERT INTO PARAMETRIZADO.TABLES_DQ 
            Select Unique  
              Tbl_Nm
              ,Clmn_Nm
          From Parametrizado.Data_Quality_Rules_2
        ';
        Commit;
        Dbms_Output.Put_Line ('Tabla catalogo creada');
      End;
    END;
    
    -- ---------------------------------------------------------------------------
    --Eliminar La Tabla Parametrizado.Data_Quality_Rules_Temp Si Existe
    -- ---------------------------------------------------------------------------
    Begin 
      
      Execute Immediate 'TRUNCATE TABLE Parametrizado.Data_Quality_Rules_Temp drop storage';
      Dbms_Output.Put_Line ('Tabla parametrizada temporal borrada');
      Begin
           -- Crear La Tabla Parametrizado.Data_Quality_Rules Actualizada 
            Execute Immediate '
              insert into Parametrizado.Data_Quality_Rules_TEMP 
              Select 
                  Rl_Nmbr
                  ,Aws_Accnt
                  ,Srvc_Dq
                  ,Dtbs
                  ,Cntr
                  ,Tbl_Nm2 As Tbl_Nm
                  ,Clmn_Nm
                  ,Rl_Nm
                  ,Dq_Dmnsn
                  ,Qlt_Rl
                  ,Implmntd_Rl
                  ,Trgt
                  ,Aws_Rl
                  ,Implmntd_Aws_Rl
                  ,Ctrl_Md5
                  ,Eff_Dt
                  ,Ppn_Dt
                  From (
                  Select A.*, Row_Number () Over (Partition By Aws_Accnt, Srvc_Dq, Dtbs, Cntr, B.Tbl_Nm, A.Clmn_Nm, Rl_Nm Order By Ppn_Dt Desc) Seq_No, B.Tbl_Nm As Tbl_Nm2
                  From Parametrizado.Data_Quality_Rules_2 A
                  Inner Join Parametrizado.Tables_Dq B On A. Clmn_Nm=B.Clmn_Nm
                  ) Where  Seq_No=1 AND srvc_dq = ''Oracle'' AND Trgt = ''Mobile'' '; --
                Commit;

            Dbms_Output.Put_Line ('Tabla parametrizada creada');
            Exception
              When Others Then
                Dbms_Output.Put_Line ('Error al crear la tabla: ' || Sqlerrm);
                Raise;
      End;
    End;
    
    
    -- -------------------------------------------------------------------------
    -- Particionamiento DE LA TABLA DE RESULTADOS
    -- -------------------------------------------------------------------------
            Begin

                    Select Count(1)  Into Ln_Prttn
                    From  Sys.All_Objects
                    Where Object_Name = Lv_Tablatrgt
                    And Owner = Lv_Esquematrgt
                    And Subobject_Name = 'P'||To_Char(P_Fecha,'YYYYMMDD')
                    And Object_Type = 'TABLE PARTITION';


                If Ln_Prttn=0 Then

                            Begin
                                Dbms_Output.Put_Line( 'ALTER TABLE  ' || Lv_Esquematrgt||'.'||Lv_Tablatrgt ||' ADD PARTITION P'|| To_Char(P_Fecha,'YYYYMMDD') ||
                                 ' VALUES ( ' || To_Char( P_Fecha, 'YYYYMMDD') || ' )' );
                                Execute Immediate 'ALTER TABLE  ' || Lv_Esquematrgt||'.'||Lv_Tablatrgt ||' ADD PARTITION P'|| To_Char(P_Fecha,'YYYYMMDD') ||
                               ' VALUES (TO_DATE( ' || Chr(39) || To_Char(P_Fecha,'YYYY-MM-DD') || ' 00:00:00'|| Chr(39) || ', '|| Chr(39) || 'SYYYY-MM-DD HH24:MI:SS'|| Chr(39) || ', ' ||
                              Chr(39)|| 'NLS_CALENDAR=GREGORIAN'|| Chr(39) || '))';
                            End;
                Else
                          Dbms_Output.Put_Line( 'ALTER TABLE ' || Lv_Esquematrgt||'.'||Lv_Tablatrgt ||' TRUNCATE PARTITION P'||To_Char(P_Fecha,'YYYYMMDD')|| ' DROP STORAGE' );

                          Execute Immediate 'ALTER TABLE ' || Lv_Esquematrgt||'.'||Lv_Tablatrgt ||' TRUNCATE PARTITION P'||To_Char(P_Fecha,'YYYYMMDD')|| ' DROP STORAGE';
                          Null;
                End If;
                    --
           Exception
                    When No_Data_Found Then
                    Null;
           End;

  -- -------------------------------------------------------------------------
    -- Recorre El Cursor De Los Registros En La Tabla Temporal
    -- ------------------------------------------------------------------------- 
    For X In (Select * From Parametrizado.Data_Quality_Rules_Temp) Loop
      Begin
    ----------------------------------------------------------------------------
    --Determinar El Tipo De Consulta
    -- -------------------------------------------------------------------------
      If Substr(Upper(X.Implmntd_Rl), 1,5) = 'WHERE' Then
        --Crear La Consulta Para Validar La Regla
          Lv_Query := Replace(X.Implmntd_Rl, ':Ev_date', '''' || Ev_Date || '''');
          Lv_Query := 'SELECT COUNT (*) FROM '|| X.Dtbs || '.' || X.Tbl_Nm || ' ' || Lv_Query || ' AND Fct_Dt = To_Date(''' || Ev_Date || ''',''yyyymmdd'')';
     
      Elsif Substr(Upper(X.Implmntd_Rl), 1,6) = 'SELECT' Then
          Lv_Query := Replace(X.Implmntd_Rl, ':Ev_date', '''' || Ev_Date || '''');
          Lv_Query := Replace(Lv_Query, ':Ant_date', '''' || Ant_Date || '''');
          Lv_Query := Replace(Lv_Query, '{esq_nm}', X.Dtbs);
          Lv_Query := Replace(Lv_Query, '{tbl_nm}', X.Tbl_Nm);
          
      Elsif Substr(Upper(X.Implmntd_Rl), 1,4) = 'WITH' Then
          Lv_Query := Replace(X.Implmntd_Rl, ':DiaMesAnterior','''' || Ev_DiaMesAnterior || '''');
          Lv_Query := Replace(Lv_Query, ':UltimoDiaMesAnterior','''' || Ev_UltimoDiaMesAnterior || '''');
          Lv_Query := Replace(Lv_Query, ':PrimerDia','''' || Ev_PrimerDia || '''');
          Lv_Query := Replace(Lv_Query, ':UltimoDia','''' || Ev_UltimoDia || '''');
          Lv_Query := Replace(Lv_Query, '{esq_nm}', X.Dtbs);
          Lv_Query := Replace(Lv_Query, '{tbl_nm}', X.Tbl_Nm);
          
      Else
          Lv_Query := 'SELECT COUNT (*) FROM '|| X.Dtbs || '.' || X.Tbl_Nm || ' ' || X.Implmntd_Rl || ' AND Fct_Dt = To_Date(''' || Ev_Date || ''',''yyyymmdd'')';
      End If;
    -- -------------------------------------------------------------------------
    -- Realiza El Conteo Total De Los Registros En La Tabla 
    -- -------------------------------------------------------------------------
      Lv_Query_Count := 'SELECT COUNT (*) FROM '|| X.Dtbs || '.' || X.Tbl_Nm || '  WHERE Fct_Dt = To_Date(''' || Ev_Date || ''',''yyyymmdd'')'; 
      --Ejecutar La Consulta Del Conteo
      Execute Immediate Lv_Query_Count Into Ln_Total;
      -- Mostrar La Consulta Generada Para Validar 
      Dbms_Output.Put_Line ('Query: ' || Lv_Query);
      
    -- -------------------------------------------------------------------------  
    --Ejecutar La Consulta De Cada Regla
    -- -------------------------------------------------------------------------
      Execute Immediate Lv_Query Into Ln_Count;
      
    -- -------------------------------------------------------------------------
    -- Mostrar Los Conteos De Errores Obtenidos
    -- -------------------------------------------------------------------------
      Dbms_Output.Put_Line ('Conteo de errores: ' || Ln_Count);
    
    -- -------------------------------------------------------------------------  
    -- Mostrar El Total De Registros
    -- -------------------------------------------------------------------------
      Dbms_Output.Put_Line ('Total registros: ' || Ln_Total);
    
    -- -------------------------------------------------------------------------
    -- Guardar Los Resultados En La Tabla De Resultados
    -- -------------------------------------------------------------------------
      Insert Into Smy_Col.Results_Dq (Dtbs_Nm, Srvc_Dq, Cntr, Tbl_Nm, Clmn_Nm, Rl_Nm, Dq_Dmnsn, Qlt_Rl, Implmntd_Rl, Trgt, Fls_Cnt, Ttl_Cnt, Eff_Dt)
      Values (X.Dtbs, X.Srvc_Dq, X.Cntr, X.Tbl_Nm, X.Clmn_Nm, X.Rl_Nm, X.Dq_Dmnsn, X.Qlt_Rl, X.Implmntd_Rl, X.Trgt, Ln_Count, Ln_Total, P_Fecha);
      EXCEPTION
        WHEN OTHERS THEN
          Declare
            V_Err_Code Number := Sqlcode;
            V_Err_Msg  Varchar2(4000) := Substr(Sqlerrm, 1, 4000);
            
          Begin
            -- Manejar errores específicos
            If V_Err_Code = -942 Then -- ORA-00942: Tabla o vista no existe
              Dbms_Output.Put_Line('Error: La tabla ' || X.Dtbs || '.' || X.Tbl_Nm || ' no existe.');
            Elsif V_Err_Code = -904 Then -- ORA-00904: Columna no válida
              Dbms_Output.Put_Line('Error: Columna no válida en la tabla ' || X.Dtbs || '.' || X.Tbl_Nm || '.');
            Else
              Dbms_Output.Put_Line('Error desconocido: ' || V_Err_Msg);
            End If;
          End;
      END;
    End Loop;
    
  Exception
    When others then
        Declare
            V_Err_Code Number := Sqlcode;
            V_Err_Msg  Varchar2(4000) := Substr(Sqlerrm, 1, 4000);
        Dbms_Output.Put_Line ('Error ' || V_Err_Msg);
        Raise;
End;