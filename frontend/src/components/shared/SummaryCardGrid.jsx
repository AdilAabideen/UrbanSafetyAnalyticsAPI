function SummaryCardGrid({ cards }) {
  return (
    <div className="grid gap-2 md:grid-cols-3 xl:grid-cols-6">
      {cards.map((card) => (
        <article
          key={card.label}
          className="rounded-[20px] border border-white/5 bg-[#030b0e]/90 p-3 shadow-2xl"
        >
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
            {card.label}
          </p>
          <p className={`mt-3 text-lg font-semibold leading-snug ${card.accent}`}>
            {card.value}
          </p>
          <p className="mt-1 text-xs leading-5 text-cyan-100/60">{card.meta}</p>
        </article>
      ))}
    </div>
  );
}

export default SummaryCardGrid;
