import { useId, useLayoutEffect, useRef } from "react";

const Modal = ({ isOpen, onClose, title, children, dismissible = true }) => {
  const dialogRef = useRef(null);
  const titleRef = useRef(null);
  const titleId = useId();

  useLayoutEffect(() => {
    if (!isOpen) return;
    const dialog = dialogRef.current;
    const document = dialog.ownerDocument;
    const opener = document.activeElement;
    const previousOverflow = document.body.style.overflow;

    // showModal (rather than the open attribute) makes the background inert
    // and places the dialog in the browser's modal layer.
    dialog.showModal();
    titleRef.current.focus({ preventScroll: true });
    document.body.style.overflow = "hidden";

    return () => {
      dialog.close();
      document.body.style.overflow = previousOverflow;
      if (opener?.isConnected) opener.focus({ preventScroll: true });
    };
  }, [isOpen]);

  useLayoutEffect(() => {
    // Save disables the focused control along with the rest of the form.
    if (isOpen && !dismissible) titleRef.current.focus({ preventScroll: true });
  }, [isOpen, dismissible]);

  if (!isOpen) return null;

  const requestClose = () => {
    if (dismissible) onClose();
  };

  return (
    <dialog
      ref={dialogRef}
      aria-modal="true"
      aria-labelledby={titleId}
      className="m-auto bg-white border-0 p-0 rounded-lg shadow-lg max-w-[630px] w-[90%] max-h-[90vh] overflow-y-auto backdrop:bg-black/50"
      onKeyDown={(event) => {
        if (event.key !== "Tab" || event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey) return;
        const dialog = event.currentTarget;
        const controls = Array.from(dialog.querySelectorAll("a[href], button, input, select, textarea, [tabindex]"))
          .filter(element => element.tabIndex >= 0 && !element.matches(":disabled") &&
            !element.closest("[hidden], [inert]") && element.getClientRects().length > 0 &&
            getComputedStyle(element).visibility !== "hidden");
        const active = dialog.ownerDocument.activeElement;
        if (!controls.length) {
          event.preventDefault();
          titleRef.current.focus();
        } else if (!controls.includes(active) ||
            (event.shiftKey ? active === controls[0] : active === controls[controls.length - 1])) {
          event.preventDefault();
          (event.shiftKey ? controls[controls.length - 1] : controls[0]).focus();
        }
      }}
      onCancel={(event) => {
        // Keep this controlled by isOpen and respect an in-progress Save.
        event.preventDefault();
        requestClose();
      }}
      onClick={(event) => {
        if (event.target !== event.currentTarget) return;
        const rect = event.currentTarget.getBoundingClientRect();
        if (event.clientX < rect.left || event.clientX > rect.right ||
            event.clientY < rect.top || event.clientY > rect.bottom) requestClose();
      }}
    >
      <div className="flex justify-between items-center px-6 py-5 border-b border-gray-200">
        <h2 ref={titleRef} id={titleId} tabIndex={-1} className="text-xl font-semibold text-gray-800 outline-none">{title}</h2>
        <button
          type="button"
          aria-label="Close dialog"
          disabled={!dismissible}
          className="bg-transparent border-none text-2xl cursor-pointer text-gray-500 w-8 h-8 flex items-center justify-center rounded hover:bg-gray-100 hover:text-gray-800 transition-all disabled:opacity-50 disabled:cursor-not-allowed focus-visible:outline-2 focus-visible:outline-blue-600"
          onClick={requestClose}
        >
          ✖
        </button>
      </div>
      {children}
    </dialog>
  );
};

export default Modal;
