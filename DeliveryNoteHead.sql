SELECT TOP 10 LFS_ORIGNR [CreatedByBranch]
      ,LFS_ANG_ANR [CustomerOrderNumber]
      ,LFS_LFS [DeliveryNoteNumber]
      ,LFS_EXECNR [ExecutionBranch]
      ,LFS_VONNR [DeliveryBy]
      ,LFS_KTYP [AddressType]
      ,LFS_KNR [CustomerOrAddressNumber]
      ,LFS_REDIR_AN_FILIALE [DifferentReceivingBranch]
      ,LFS_ROLLEND [StockInTransitUsed]
      ,LFS_RNR_ORIGNR [InvoiceBranch]
      ,LFS_RNR [InvoiceNumber]
      ,LFS_DATLFS [DeliveryNoteDate]
      ,LFS_DATOK [DeliveryNoteConfirmationDate]
      ,LFS_DATFREI [DeliveryNoteReleaseDate]
      ,LFS_DATBUCH [DeliveryNoteBookingDate]
      ,LFS_DATBUCH_TIME [DeliveryNoteExecutionTime]
      ,LFS_DATRG [InvoiceDate]
      ,LFS_UST [CalculateGST]
      ,LFS_KST [CostCentre]
      ,LFS_TEXT [Text]
      ,LFS_HINWEIS_1 [Note1]
      ,LFS_HINWEIS_2 [Note2]
      ,LFS_SACH [PersonInCharge]
      ,LFS_VERTRETER [SalesRepresentative]
      ,LFS_PROVISION [Commission]
      ,LFS_PROV_ANTEIL [CommissionPercentage]
      ,LFS_PACKSTCK [NoOfPackages]
      ,LFS_GES_KG [WeightKgs]
      ,LFS_PREIS_BRUTTO [PriceGross]
      ,LFS_PREISSTUFE [PriceBreak]
      ,LFS_DATUM_LAGER [BookingDateOfStockData]
      ,LFS_DATUM_STAT [BookingDateOfStatistics]
      ,LFS_DATUM_BONUS [BonusBookingDate]
      ,LFS_DATUM_USER [ExportDateSpecialProgram]
      ,LFS_EXPORT_DATUM [ExportDate]
      ,LFS_IMPORT_DATUM [ImportDate]
      ,LFS_LADELISTE_DATUM [LoadingListDate]
      ,LFS_SKONTO [Discount]
      ,LFS_SKONTO_FRIST [EarlyPaymentPeriod]
      ,LFS_VORFRACHT_PROZ[OriginalFreightPercentage]
      ,LFS_VORFRACHT_NETTO[OriginalFreightNet] 
      ,LFS_VORFRACHT_BRUTTO [OriginalFreightGross]
      ,LFS_TRANSVERS_PROZ [TransportInsurancePerventage]
      ,LFS_TRANSVERS_NETTO [TransportInsuranceNet]
      ,LFS_TRANSVERS_BRUTTO [TransportInsuranceGross]
      ,LFS_TRANSPORT_NETTO [TransportCostsNet]
      ,LFS_TRANSPORT_BRUTTO [TransportCostsGross]
      ,LFS_INT_TRANSPORT_PROZ [InternalTransportCostsPercentage]
      ,LFS_INT_TRANSPORT_NETTO [InternalTransportCostsNet]
      ,LFS_INT_TRANSPORT_BRUTTO [InternalTransportCostsGross]
      ,LFS_INT_SONSTIG_PROZ [OtherInternalCostsPercentage]
      ,LFS_INT_SONSTIG_NETTO [OtherInternalCostsNet]
      ,LFS_INT_SONSTIG_BRUTTO [OtherInternalCosts[Gross]
      ,LFS_VERSANDART [ShippingMethod]
      ,LFS_PACKART [PackagingType]
      ,LFS_ZAHLART [PaumentType]
      ,LFS_VALUTA_DATUM [ValueDate]
      ,LFS_ZAHLUNGSZIEL [PaymentLine]
      ,LFS_REF_FILIALE [IBTConfirmationBranch]
      ,LFS_REF_KASSE [IBTConfirmationPOS]
      ,LFS_REF_NUMMER [IBTConfirmationNumber]
      ,LFS_BELEG_TYP [DocumentType]
      ,LFS_BELEG_FILIALE [DocumentBranch]
      ,LFS_BELEG_KASSE [DocumentPos]
      ,LFS_BELEG_NUMMER [DocumentNumber]
      ,LFS_DOCCNT_TYP [DocumentType]
      ,LFS_DOCCNT_MAPPING [DocumentMapping]
      ,LFS_DOCCNT_ORIGNR [DocumentBranch]
      ,LFS_DOCCNT_NUMMER [DocumentNumber]
      ,LFS_DOCCNT_COUNT [DocumentCounter]
      ,LFS_DOCCNT_INFO [DocumentSInfo]
      ,LFS_STATUS [Status]
      ,LFS_RABATT [Discount]
      ,LFS_PREISLINIE [PriceCode]
      ,LFS_KLASSIFIZIERUNG [Classification]
      ,LFS_WAEHRUNG [Currency]
      ,LFS_DRUCKWAEHRUNG [PrintCurrency]
      ,LFS_VON_OWNER_NUMMER [OwnerOfSendingBranch]
      ,LFS_AN_OWNER_NUMMER [OwnerOfReceivingBranch]
      ,LFS_GEBUCHT_ALS [BookingMode]
      ,LFS_KURS [ExchangeRate]
      ,LFS_KURSDATUM [RateDate]
      ,LFS_KURS_FESTFLAG [FixedRate]
      ,LFS_TEILLIEFERUNG [PartialDelevery]
      ,LFS_ABHOLNR [PickUpBy]
      ,LFS_TOUR [Route]
      ,LFS_VFW_KOSTEN [PfrCosts]
      ,LFS_DOK_DOKTYP [DocumentType]
      ,LFS_EDI_DOKTYP [DocumentTypeEDI]
      ,LFS_EDI_DESADV_DATUM [DEASEDVDate]
      ,LFS_EDI_TAG [ExportDateDay]
      ,LFS_EDI_WOCHE [ExportDateWeek]
      ,LFS_EDI_MONAT [ExportDateMonth]
      ,LFS_EDI_IO_DATUM [EdiIODate]
      ,LFS_EDI_IO_DOKID [EdiIODocument]
      ,LFS_EDI_RECADV_RCV_DATUM [RECADVReceiptDate]
      ,LFS_EDI_RECADV_RCV_DOKID [RECADVDocumentReceived]
      ,LFS_EDI_RECADV_SND_DATUM [RECADVSendDate]
      ,LFS_EDI_RECADV_SND_DOKID [RECADVDocumentSent]
      ,LFS_SSCC [ShippingContainer]
      ,LFS_EXTERNID [ExternalID]
      ,LFS_VERKAEUFER [SalesPerson]
      ,LFS_RG_DRUCKFORMAT[InvoiceFormat]
      ,LFS_RG_DRUCKRABATT [InvoiceWithDiscount]
      ,LFS_RG_ALS_EMPFAENGER [CreditNoteForCustomer]
      ,LFS_EPC_CONTENT [EPCsContained]
      ,LFS_EPC_IGNORE [DoNotBookEPCs]
      ,LFS_USERSTATE [UserStatus]
      ,LFS_MESSAGE_ID [MessageId]
      ,LFS_DW_DATUM [ExportDateDataWerahouse]
      ,LFS_WF_STATUS [WorkflowStatus]
      ,LFS_WF_FLAGS [WorkFlowFlags]
      ,LFS_WF_ID [WorkflowLogId]
      ,LFS_WF_DATE_1 [WorkflowDate_1]
      ,LFS_WF_TIME_1 [WorkflowTime_1]
      ,LFS_WF_DATE_2 [WorkflowDate_2]
      ,LFS_WF_TIME_2 [WorkflowTime_2]
      ,LFS_WF_DATE_3 [WorkflowDate_3]
      ,LFS_WF_TIME_3 [WorkflowTime_3]
      ,LFS_INVERT_ORIGNR [InversionOfCreatedInBranchRef]
      ,LFS_INVERT_ANG_ANR [InversionOfCrreatedOrderNo]
      ,LFS_INVERT_LFS [InversionOfDeliveryNote]
      ,LFS_ARCHIVE_DATE [ArchiveDate]
      ,LFS_ARCHIVE_UUID [ArchiveId]
      ,LFS_CLOG_USER [CreatedBy]
      ,LFS_CLOG_DATE [DateCreated]
      ,LFS_CLOG_TIME [TimeCreated]
      ,LFS_ULOG_USER [UpdatedBy]
      ,LFS_ULOG_DATE [UpdatedDate]
      ,LFS_ULOG_TIME [UpdatedTime]
     
  FROM FUTURA.dbo.LIEFHEAD where LFS_KTYP = 3  AND LFS_TEXT like 'ODS_%'