import ReportedEventPage from "./ReportedEventPage";

function ReportCrimePage({ docsUrl, accessToken, onReportCreated }) {
  return (
    <ReportedEventPage
      docsUrl={docsUrl}
      accessToken={accessToken}
      eventKind="crime"
      title="Report Crime"
      subtitle="Capture a public crime report with precise coordinates, event timing, and a short narrative."
      onReportCreated={onReportCreated}
    />
  );
}

export default ReportCrimePage;
