function EmptyAnalyticsState({ message }) {
  return (
    <div className="flex h-full min-h-[320px] items-center justify-center px-6 text-center">
      <div>
        <p className="text-lg font-semibold text-cyan-50">Nothing to plot yet</p>
        <p className="mt-2 text-sm text-cyan-100/60">{message}</p>
      </div>
    </div>
  );
}

export default EmptyAnalyticsState;
