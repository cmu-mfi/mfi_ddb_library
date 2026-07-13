const Modal = ({ isOpen, onClose, title, children }) => {
  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 bg-black/50 flex items-center justify-center z-50"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-lg shadow-lg max-w-[630px] w-[90%] max-h-[90vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-between items-center px-6 py-5 border-b border-gray-200">
          <h2 className="text-xl font-semibold text-gray-800">{title}</h2>
          <button
            className="bg-transparent border-none text-2xl cursor-pointer text-gray-500 w-8 h-8 flex items-center justify-center rounded hover:bg-gray-100 hover:text-gray-800 transition-all"
            onClick={onClose}
          >
            ✖
          </button>
        </div>
        {children}
      </div>
    </div>
  );
};

export default Modal;
