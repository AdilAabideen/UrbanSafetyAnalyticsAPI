import ReportedEventPage from "./ReportedEventPage";

function ReportCollisionPage({ docsUrl, accessToken, onReportCreated }) {
  return (
    <ReportedEventPage
      docsUrl={docsUrl}
      accessToken={accessToken}
      eventKind="collision"
      title="Report Collision"
      subtitle="Submit a public collision report with conditions, vehicle count, and a verified location point."
      onReportCreated={onReportCreated}
    />
  );
}

export default ReportCollisionPage;
