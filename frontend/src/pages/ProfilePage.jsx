import { useEffect, useMemo, useState } from "react";

function ProfilePage({
  loading,
  user,
  onRefresh,
  onUpdateProfile,
  onLogout,
  onBackToMap,
}) {
  
  const [email, setEmail] = useState(user?.user.email || "");
  const [password, setPassword] = useState("");
  const [saving, setSaving] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [feedback, setFeedback] = useState(null);

  useEffect(() => {
    setEmail(user?.user.email || "");
  }, [user?.user.email]);

  const createdAtLabel = useMemo(() => {
    if (!user?.user.created_at) {
      return "Unavailable";
    }

    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "short",
    }).format(new Date(user?.user.created_at));
  }, [user?.user.created_at]);

  async function handleRefresh() {
    setRefreshing(true);
    setFeedback(null);

    try {
      await onRefresh();
      setFeedback({ tone: "info", text: "Profile reloaded from GET /me." });
    } catch (error) {
      setFeedback({
        tone: "error",
        text: error?.message || "Could not refresh the current user.",
      });
    } finally {
      setRefreshing(false);
    }
  }

  async function handleSubmit(event) {
    event.preventDefault();

    if (password.trim() && password.trim().length < 8) {
      setFeedback({
        tone: "error",
        text: "New passwords must be at least 8 characters long.",
      });
      return;
    }

    const payload = {};

    if (email.trim() && email.trim() !== user?.user.email) {
      payload.email = email.trim();
    }

    if (password.trim()) {
      payload.password = password.trim();
    }

    if (!Object.keys(payload).length) {
      setFeedback({
        tone: "error",
        text: "Provide a new email, a new password, or both before saving.",
      });
      return;
    }

    setSaving(true);
    setFeedback(null);

    try {
      const updatedUser = await onUpdateProfile(payload);
      setEmail(updatedUser.user.email);
      setPassword("");
      setFeedback({ tone: "success", text: "Profile updated successfully." });
    } catch (error) {
      setFeedback({
        tone: "error",
        text: error?.message || "The profile update failed.",
      });
    } finally {
      setSaving(false);
    }
  }

  if (loading) {
    return (
      <div className="grid h-full w-full place-items-center bg-[#071316] px-4 py-10">
        <div className="w-full max-w-[420px] rounded-2xl border border-cyan-100/10 bg-[#030b0e] p-6 text-center shadow-2xl">
          <p className="text-sm text-cyan-100/60">Loading profile...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="grid h-full w-full place-items-center bg-[#071316] px-4 py-10">
        <div className="w-full max-w-[420px] rounded-2xl border border-cyan-100/10 bg-[#030b0e] p-6 text-center shadow-2xl">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-100/45">
            Not signed in
          </p>
          <p className="mt-3 text-sm text-cyan-100/60">
            Log in to view your profile.
          </p>
          <div className="mt-5 flex justify-center gap-3">
            <ActionButton variant="primary" onClick={() => window.location.href = "/login"}>
              Go to Login
            </ActionButton>
            <ActionButton variant="secondary" onClick={onBackToMap}>
              Back to Map
            </ActionButton>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full w-full overflow-y-auto bg-[#071316] px-4 py-10">
      <section className="mx-auto w-full max-w-[520px] rounded-2xl border border-cyan-100/10 bg-[#030b0e] p-6 shadow-2xl">
        <div className="flex items-center justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-100/45">
              Profile
            </p>
            <p className="mt-2 text-sm text-cyan-100/60">Urban Risk Intelligence System</p>
          </div>
          <button
            type="button"
            onClick={handleRefresh}
            disabled={refreshing}
            className="rounded-lg bg-cyan-100/10 px-3 py-2 text-xs font-medium text-cyan-50 transition-colors hover:bg-cyan-100/15 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {refreshing ? "Refreshing..." : "Refresh"}
          </button>
        </div>

        <div className="mt-6 space-y-2 rounded-lg border border-cyan-100/10 bg-cyan-100/5 p-4 text-sm text-cyan-50">
          <p>User ID: {user?.user.id}</p>
          <p>Email: {user?.user.email}</p>
          <p>Admin: {user?.user.is_admin ? "Yes" : "No"}</p>
          <p>Created: {createdAtLabel}</p>
        </div>

        {feedback ? <NoticeBanner notice={feedback} className="mt-4" /> : null}

        <form className="mt-6 flex flex-col gap-4" onSubmit={handleSubmit}>
          <Field
            id="profile-email"
            label="Email"
            type="email"
            autoComplete="email"
            value={email}
            placeholder="updated@example.com"
            onChange={setEmail}
          />
          <Field
            id="profile-password"
            label="New Password"
            type="password"
            autoComplete="new-password"
            value={password}
            placeholder="Leave blank to keep your current password"
            minLength={password ? 8 : undefined}
            onChange={setPassword}
          />

          <div className="grid gap-3 sm:grid-cols-3">
            <ActionButton type="button" variant="secondary" onClick={onBackToMap}>
              Back to Map
            </ActionButton>
            <ActionButton type="submit" variant="primary" disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </ActionButton>
            <ActionButton type="button" variant="danger" onClick={onLogout}>
              Sign out
            </ActionButton>
          </div>
        </form>
      </section>
    </div>
  );
}

function Field({ id, label, type, autoComplete, value, placeholder, minLength, onChange }) {
  return (
    <label className="flex flex-col gap-2" htmlFor={id}>
      <span className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-100/45">
        {label}
      </span>
      <input
        id={id}
        type={type}
        autoComplete={autoComplete}
        value={value}
        placeholder={placeholder}
        minLength={minLength}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-lg border border-cyan-100/10 bg-cyan-100/5 px-4 py-3 text-sm text-cyan-50 outline-none transition-colors focus:border-cyan-100/30 focus:bg-cyan-100/10"
      />
    </label>
  );
}

function ActionButton({ children, className = "", variant, ...props }) {
  const variantClass = {
    primary: "bg-cyan-100/10 text-cyan-50 hover:bg-cyan-100/15",
    secondary: "bg-cyan-100/5 text-cyan-50 hover:bg-cyan-100/10",
    danger: "bg-red-500/10 text-red-100 hover:bg-red-500/15",
  }[variant];

  return (
    <button
      className={`rounded-lg px-4 py-3 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${variantClass} ${className}`}
      {...props}
    >
      {children}
    </button>
  );
}

function NoticeBanner({ notice, className = "" }) {
  const toneClass = {
    success: "border-[#39ef7d]/40 bg-[#39ef7d]/10 text-[#d8ffe7]",
    error: "border-red-300/40 bg-[#521010]/70 text-red-100",
    info: "border-[#8df7ff]/30 bg-[#8df7ff]/10 text-cyan-50",
  }[notice.tone || "info"];

  return (
    <div className={`rounded-lg border px-4 py-3 text-sm ${toneClass} ${className}`}>
      {notice.text}
    </div>
  );
}

export default ProfilePage;
