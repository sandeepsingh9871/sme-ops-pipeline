Attribute VB_Name = "WeeklySummary"
Option Explicit

' Folder holding the CSVs exported by etl/export_for_bi.py (copy of reports/bi/).
' Change this if your folder is different. Keep the trailing backslash.
Private Const DATA_FOLDER As String = "C:\Users\grimm\Documents\sme_bi\"

Public Sub BuildWeeklySummary()
    Dim wb As Workbook, wsData As Worksheet, wsSum As Worksheet
    Dim src As Workbook, co As ChartObject
    Dim products As Variant, i As Long, r As Long, pdfPath As String

    On Error GoTo Fail
    Set wb = ThisWorkbook
    Application.ScreenUpdating = False
    Application.DisplayAlerts = False

    ' ---- 1. Load the latest monthly_summary.csv into the Data sheet --------------
    Set wsData = GetOrCreateSheet(wb, "Data")
    wsData.Cells.Clear
    Set src = Workbooks.Open(DATA_FOLDER & "monthly_summary.csv", ReadOnly:=True)
    src.Worksheets(1).UsedRange.Copy wsData.Range("A1")
    src.Close SaveChanges:=False
    wsData.Columns("A").NumberFormat = "yyyy-mm-dd"
    wsData.Columns("A:H").AutoFit

    ' ---- 2. Build the Summary sheet with live formulas -----------------------------
    Set wsSum = GetOrCreateSheet(wb, "Summary")
    wsSum.Cells.Clear
    For i = wsSum.ChartObjects.Count To 1 Step -1
        wsSum.ChartObjects(i).Delete
    Next i

    products = Array("Checking or savings account", "Credit card", _
                     "Credit card or prepaid card", "Prepaid card", _
                     "Money transfer, virtual currency, or money service")

    With wsSum
        .Range("A1").Value = "SME servicing: weekly complaint summary"
        .Range("A1").Font.Size = 14
        .Range("A1").Font.Bold = True
        .Range("A2").Value = "Data through (month):"
        .Range("B2").Formula = "=MAX(Data!A:A)"
        .Range("B2").NumberFormat = "mmm yyyy"
        .Range("B2").HorizontalAlignment = xlLeft

        .Range("A4:H4").Value = Array("Product", "Complaints (all)", "Late responses (all)", _
            "Late rate (all)", "Complaints (last 12m)", "Late rate (last 12m)", _
            "Late rate (prior 12m)", "Change (pts)")

        For i = 0 To UBound(products)
            r = 5 + i
            .Cells(r, 1).Value = products(i)
            .Cells(r, 2).Formula = "=SUMIFS(Data!$E:$E,Data!$D:$D,$A" & r & ")"
            .Cells(r, 3).Formula = "=SUMIFS(Data!$F:$F,Data!$D:$D,$A" & r & ")"
            .Cells(r, 4).Formula = "=IFERROR(C" & r & "/B" & r & ",0)"
            .Cells(r, 5).Formula = "=SUMIFS(Data!$E:$E,Data!$D:$D,$A" & r & _
                ",Data!$A:$A,"">""&EDATE($B$2,-12))"
            .Cells(r, 6).Formula = "=IFERROR(SUMIFS(Data!$F:$F,Data!$D:$D,$A" & r & _
                ",Data!$A:$A,"">""&EDATE($B$2,-12))/E" & r & ",0)"
            .Cells(r, 7).Formula = "=IFERROR(SUMIFS(Data!$F:$F,Data!$D:$D,$A" & r & _
                ",Data!$A:$A,"">""&EDATE($B$2,-24),Data!$A:$A,""<=""&EDATE($B$2,-12))" & _
                "/SUMIFS(Data!$E:$E,Data!$D:$D,$A" & r & _
                ",Data!$A:$A,"">""&EDATE($B$2,-24),Data!$A:$A,""<=""&EDATE($B$2,-12)),0)"
            .Cells(r, 8).Formula = "=(F" & r & "-G" & r & ")*100"
        Next i

        ' Total row
        .Range("A10").Value = "Total"
        .Range("B10").Formula = "=SUM(B5:B9)"
        .Range("C10").Formula = "=SUM(C5:C9)"
        .Range("D10").Formula = "=IFERROR(C10/B10,0)"
        .Range("E10").Formula = "=SUM(E5:E9)"
        .Range("F10").Formula = "=IFERROR(SUMPRODUCT(F5:F9,E5:E9)/E10,0)"
        .Range("G10").Formula = "=IFERROR(SUMIFS(Data!$F:$F,Data!$A:$A,"">""&EDATE($B$2,-24)," & _
            "Data!$A:$A,""<=""&EDATE($B$2,-12))/SUMIFS(Data!$E:$E,Data!$A:$A,"">""&EDATE($B$2,-24)," & _
            "Data!$A:$A,""<=""&EDATE($B$2,-12)),0)"
        .Range("H10").Formula = "=(F10-G10)*100"

        ' Formatting
        .Range("B5:C10,E5:E10").NumberFormat = "#,##0"
        .Range("D5:D10,F5:G10").NumberFormat = "0.00%"
        .Range("H5:H10").NumberFormat = "+0.00;-0.00;0.00"
        With .Range("A4:H4")
            .Font.Bold = True
            .Interior.Color = RGB(221, 235, 247)
            .WrapText = True
            .VerticalAlignment = xlCenter
        End With
        .Range("A10:H10").Font.Bold = True
        .Range("A10:H10").Borders(xlEdgeTop).LineStyle = xlContinuous
        .Columns("A").ColumnWidth = 46
        .Columns("B:H").ColumnWidth = 14

        ' Notes
        .Range("A36").Value = "Notes"
        .Range("A36").Font.Bold = True
        .Range("A37").Value = "Late = the CFPB timely-response flag is No. It is not a finding of wrongdoing."
        .Range("A38").Value = "The latest month may be partial. Change (pts) is in percentage points."
        .Range("A39").Value = "Source: CFPB Consumer Complaint Database (bulk CSV) via Hive tables sme_ops.*."
    End With

    ' ---- 3. Chart: late rate, last 12 months vs prior 12 months ---------------------
    Set co = wsSum.ChartObjects.Add(Left:=wsSum.Range("A13").Left, _
                                    Top:=wsSum.Range("A13").Top, Width:=700, Height:=300)
    With co.Chart
        .ChartType = xlColumnClustered
        .SetSourceData wsSum.Range("A4:A9,F4:G9")
        .HasTitle = True
        .ChartTitle.Text = "Late-response rate by product: last 12 months vs prior 12 months"
        .Axes(xlValue).TickLabels.NumberFormat = "0%"
        .HasLegend = True
        .Legend.Position = xlLegendPositionBottom
    End With

    ' ---- 4. Export the Summary sheet to PDF ------------------------------------------
    pdfPath = DATA_FOLDER & "weekly_summary_" & Format(Date, "yyyy-mm-dd") & ".pdf"
    With wsSum.PageSetup
        .Orientation = xlLandscape
        .Zoom = False
        .FitToPagesWide = 1
        .FitToPagesTall = 1
    End With
    wsSum.ExportAsFixedFormat Type:=xlTypePDF, Filename:=pdfPath, OpenAfterPublish:=False

    wsSum.Activate
    Application.ScreenUpdating = True
    Application.DisplayAlerts = True
    MsgBox "Summary built." & vbCrLf & "PDF saved to:" & vbCrLf & pdfPath, vbInformation
    Exit Sub

Fail:
    Application.ScreenUpdating = True
    Application.DisplayAlerts = True
    MsgBox "Error " & Err.Number & ": " & Err.Description, vbCritical
End Sub

Private Function GetOrCreateSheet(wb As Workbook, nm As String) As Worksheet
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = wb.Worksheets(nm)
    On Error GoTo 0
    If ws Is Nothing Then
        Set ws = wb.Worksheets.Add(After:=wb.Worksheets(wb.Worksheets.Count))
        ws.Name = nm
    End If
    Set GetOrCreateSheet = ws
End Function
