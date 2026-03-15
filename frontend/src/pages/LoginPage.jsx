import { useState } from "react";

function LoginPage({ notice, onLogin, onRegister }) {
  const [mode, setMode] = useState("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState(null);

  const activeNotice = feedback || (notice ? { tone: "info", text: notice } : null);

  const submitLabel = mode === "login" ? "Authenticate" : "Create account";
  const helperCopy =
    mode === "login"
      ? "Use your email address as the login identifier."
      : "Passwords must be at least 8 characters and email addresses must be unique.";

  async function handleSubmit(event) {
    event.preventDefault();

    if (!email.trim() || !password) {
      setFeedback({
        tone: "error",
        text: "Enter both an email address and a password.",
      });
      return;
    }

    if (mode === "register" && password.trim().length < 8) {
      setFeedback({
        tone: "error",
        text: "Registration passwords must be at least 8 characters long.",
      });
      return;
    }

    setSubmitting(true);
    setFeedback(null);

    try {
      if (mode === "login") {
        await onLogin({ email: email.trim(), password });
      } else {
        await onRegister({ email: email.trim(), password });
        setMode("login");
        setPassword("");
        setFeedback({
          tone: "success",
          text: "Account created. Sign in with the same email and password.",
        });
      }
    } catch (error) {
      setFeedback({
        tone: "error",
        text: error?.message || "The request failed. Check the API and try again.",
      });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[#071316] px-4 py-10">
      <section className="w-full max-w-[420px] rounded-2xl border border-cyan-100/10 bg-[#030b0e] p-6 shadow-2xl">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-100/45">
            Access Portal
          </p>
          <p className="mt-2 text-sm text-cyan-100/60">Urban Risk Intelligence System</p>
        </div>

        <div className="mt-6 inline-flex rounded-full border border-cyan-100/10 bg-cyan-100/5 p-1">
          <ModeButton
            active={mode === "login"}
            label="Login"
            onClick={() => {
              setMode("login");
              setFeedback(null);
            }}
          />
          <ModeButton
            active={mode === "register"}
            label="Register"
            onClick={() => {
              setMode("register");
              setFeedback(null);
            }}
          />
        </div>

        {activeNotice ? <NoticeBanner notice={activeNotice} /> : null}

        <form className="mt-6 flex flex-col gap-4" onSubmit={handleSubmit}>
          <Field
            id="auth-email"
            label="Email"
            value={email}
            type="email"
            autoComplete="email"
            placeholder="user@example.com"
            required
            onChange={setEmail}
          />
          <Field
            id="auth-password"
            label="Password"
            value={password}
            type="password"
            autoComplete={mode === "login" ? "current-password" : "new-password"}
            placeholder={mode === "login" ? "Enter your password" : "Minimum 8 characters"}
            minLength={mode === "register" ? 8 : undefined}
            required
            onChange={setPassword}
          />

          <p className="text-xs text-cyan-100/50">{helperCopy}</p>

          <button
            type="submit"
            disabled={submitting}
            className="rounded-lg bg-cyan-100/10 px-4 py-3 text-sm font-medium text-cyan-50 transition-colors hover:bg-cyan-100/15 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {submitting ? "Working..." : submitLabel}
          </button>
        </form>
      </section>
    </div>
  );
}

function Field({
  id,
  label,
  value,
  type,
  autoComplete,
  placeholder,
  minLength,
  required,
  onChange,
}) {
  return (
    <label className="flex flex-col gap-2" htmlFor={id}>
      <span className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-100/45">
        {label}
      </span>
      <input
        id={id}
        value={value}
        type={type}
        autoComplete={autoComplete}
        placeholder={placeholder}
        minLength={minLength}
        required={required}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-lg border border-cyan-100/10 bg-cyan-100/5 px-4 py-3 text-sm text-cyan-50 outline-none transition-colors focus:border-cyan-100/30 focus:bg-cyan-100/10"
      />
    </label>
  );
}

function ModeButton({ active, label, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full px-4 py-2 text-xs font-medium transition-colors ${
        active ? "bg-cyan-100/10 text-cyan-50" : "text-cyan-100/55 hover:text-cyan-50"
      }`}
    >
      {label}
    </button>
  );
}

function NoticeBanner({ notice }) {
  const toneClass = {
    success: "border-[#39ef7d]/40 bg-[#39ef7d]/10 text-[#d8ffe7]",
    error: "border-red-300/40 bg-[#521010]/70 text-red-100",
    info: "border-[#8df7ff]/30 bg-[#8df7ff]/10 text-cyan-50",
  }[notice.tone || "info"];

  return (
    <div className={`mt-4 rounded-lg border px-4 py-3 text-sm ${toneClass}`}>
      {notice.text}
    </div>
  );
}

export default LoginPage;
